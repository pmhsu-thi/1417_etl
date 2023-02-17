import logging
import numpy as np
import pandas as pd
from datetime import datetime

from utils import GridState
from utils import positive_list, positive_list_union, get_week_no, calc_cnt, add_event, pan_time
from args import HOURLY_DATA_COLUMNS, NAME_TYPE_TO_TABLE

class RawDataProcess:
    def charging_period_limit(self, row):
        '''
        # 限制開單時間於收費時段中
        row[0]: 格位編號
        row[1]: 車輛進入時間
        row[2]: 車輛離開時間
        '''
        grid_type = self.grid_types[row[0]]
        benchmark_start = self.charge_dict[grid_type][get_week_no(row[1].date(), self.holidays)]['period'][0][0]
        benchmark_end = self.charge_dict[grid_type][get_week_no(row[1].date(), self.holidays)]['period'][-1][-1]
        start_time, end_time = pan_time(
            grid=row[0], start=row[1], end=row[2], time_start=benchmark_start, time_end=benchmark_end
        )
        return start_time, end_time

    # 將資料轉換為 Dict
    def to_dict(self):
        '''
        dict
        {
            車輛進入
            grid_id : [infotime, 1, 計時收費判斷(1 :計時收費; 0 :非計時收費)],
            車輛離開
            grid_id : [infotime, 0, 現繳判斷(1 :現繳; 0 :非現繳), 繳費成功判斷(1 :已成功繳費; 0 :未成功繳費)],
        }
        '''
        # 將原始資料分類至不同 name_type 的車格中
        for row in self.raw_data:
            grid = row[0]
            if grid not in self.grid_name_types.keys():
                continue
            if grid not in self.grid_dict.keys():
                self.grid_dict[grid] = []

            start_time, end_time = self.charging_period_limit(row)
            if start_time is None or end_time is None:
                # logging.warning(f"grid {grid} at {row[1]} out of charging days")
                continue

            self.grid_dict[grid].append([start_time, 1])
            self.grid_dict[grid].append([end_time, 0])          

        # 將分派到各車格中的資料依時間排序
        for grid in self.grid_dict:
            self.grid_dict[grid] = sorted(self.grid_dict[grid], key=lambda x: (x[0]))


class DataTransformerHourly(RawDataProcess, GridState):
    def raw_data_process(self):
        '''
        raw_data
        [
            ('lac_code', 'start_time', 'end_time', 'park_time_unit', 'pay_status', 'pay_type', 'fee_type', 'park_mins', 'amount', 'user_pay')
        ]
        \nto\n
        grid_dict
        {
            grid_id : [infotime, event (1: enter; 0: leave)]
        }
        '''
        # 將原始資料拆分為各個格位的進出事件
        RawDataProcess()
        self.to_dict()
    
    def static_data_process(self):
        # 正向表列各種收費類型的收費小時
        charging_period = positive_list(self.charge_dict)
        # 聯集該路段所有收費類型的收費小時
        self.road_charging = positive_list_union(charging_period)
        # 計算每個小時中，有收費的格位數，用來作為分時資料中每個路段的總格位數使用
        self.road_cnt = calc_cnt(
            self.road_charging, charging_period, self.grid_static, self.grid_types, self.grid_name_types
        )
    
    # Insert timestamp to the grid dictionary data
    # When read the timestamp, record status of the grid
    def insert_timestamp(self, grid_dict, sorted_dict):
        for grid in grid_dict:
            grid_index = 0
            grid_index_stop = 0
            sorted_dict[grid] = []
            for timestamp in self.time_arr:
                weekday_type = get_week_no(timestamp.date(), self.holidays)
                # Ignore timestamp which not in charging period
                if timestamp.time() not in self.road_charging[grid[:3]][weekday_type]:
                    continue
                
                # All events have been recorded
                if grid_index_stop:
                    sorted_dict[grid].append([timestamp, -1])
                    
                # When parking event is at the same time as the timestamp, append timestamp >> parking event
                elif timestamp == grid_dict[grid][grid_index][0]:
                    sorted_dict[grid].append([timestamp, -1])
                    grid_index_stop, grid_index = add_event(timestamp, grid_dict, sorted_dict, grid, grid_index)

                # There are no parking event during this period
                elif timestamp < grid_dict[grid][grid_index][0]:
                    sorted_dict[grid].append([timestamp, -1])

                # Normal record parking event and timestamp
                elif timestamp > grid_dict[grid][grid_index][0]:
                    grid_index_stop, grid_index = add_event(timestamp, grid_dict, sorted_dict, grid, grid_index)
                    sorted_dict[grid].append([timestamp, -1])

    # 轉換為格位分時資料
    def to_grid_period_data(self, sorted_dict):
        '''
        sorted_dict 帶有分時時戳的格位進出事件資料
        {
            grid : [infotime, event_type (1: enter; 0: leave; -1: hourly_timestamp)]
        }

        park_grid 格位分時資料\n
        [grid, hourly_timestamp, used, newcar, oldcar, leavecar]
        '''
        gs = GridState()
        park_grid = []
        for grid in sorted_dict:
            # 當 state['reset'] = 1 時, 重置格位所有狀態
            gs.state['reset'] = 1
            last_event = [datetime.min, -1]
            for row in sorted_dict[grid]:
                # 日期變更，重新確認收費時段
                if row[0].date() != last_event[0].date():
                    charging_end = self.road_charging[grid[:3]][get_week_no(row[0].date(), self.holidays)][-1]
                    gs.state['reset'] = 1
                    last_event = row

                # 事件間隔時間
                delta_time = row[0] - last_event[0]
                # Reset grid state
                if gs.state['reset']:
                    gs.reset_state()

                # 車輛進入車格
                elif row[1] == 1:
                    gs.car_enter(delta_time)

                # 車輛離開車格
                elif not row[1]:
                    gs.car_leave(delta_time)

                # 碰到分時時間戳, 統整紀錄該小時數據
                elif row[1] == -1:
                    gs.write_data(park_grid, grid, row[0], delta_time)
                    # End of charging period
                    if row[0].time() == charging_end:
                        gs.state['reset'] = 1
                last_event = row

        return park_grid
    
    def park_road_add_key(self, road, name_type_table):
        '''
        出現未記錄於 park_road 路段時, 建立相對應結構
        '''
        if road in self.park_road[name_type_table].keys():
            return
        
        column_number = len(HOURLY_DATA_COLUMNS) - 2
        self.park_road[name_type_table][road] = {}
        for timestamp in self.time_arr:
            if timestamp.time() in self.road_charging[road][get_week_no(timestamp.date(), self.holidays)][1:]:
                self.park_road[name_type_table][road][timestamp] = [0 for _ in range(column_number)]

    # 格位分時資料 >> 路段分時資料
    def to_road_period_data(self, park_grid):
        '''
        park_grid 格位分時資料\n
        [grid, time, stay_time, newcar, oldcar, leavecar]\n
        park_road 路段分時資料
        {
            hourly_data_table:{ key = ['large_vehicle', 'special_vehicle', 'truck', 'vehicle']
                road :{
                    time :{
                        [totalstay, stay, usage, newcar, oldcar, totalcar, leavecar, supply, turnover, cnt]
                    }
                },
            }
        }
        '''
        for obj in park_grid:
            name_type_table = NAME_TYPE_TO_TABLE[self.grid_name_types[obj[0]]]
            road = obj[0][:3]
            time_stamp = obj[1]
            # 建立對應的 key-value 結構
            self.park_road_add_key(road, name_type_table)
            
            # Total stay
            self.park_road[name_type_table][road][time_stamp][0] += obj[2]
            # New car
            self.park_road[name_type_table][road][time_stamp][3] += obj[3]
            # Old car
            self.park_road[name_type_table][road][time_stamp][4] += obj[4]
            # Leave car
            self.park_road[name_type_table][road][time_stamp][6] += obj[5]

        for table in self.park_road:
            for road in self.park_road[table]:
                for timestamp in self.park_road[table][road]:
                    obj = self.park_road[table][road][timestamp]
                    try:
                        week_no = get_week_no(timestamp.date(), self.holidays)
                        cnt = self.road_cnt[table][road][week_no][timestamp.time()]
                    except ValueError as err:
                        logging.critical(
                            f'There are no Cnt in road_cnt[{table}][{road}][{week_no}][{timestamp.time()}]'
                        )
                        raise err
                    # Cnt from road_cnt (utils.calc_cnt)
                    self.park_road[table][road][timestamp][-1] = cnt
                    # Stay = Total stay / Cnt
                    self.park_road[table][road][timestamp][1] = obj[0] / cnt
                    # Usage = Total stay / 最大可用時間 (總格位數 * 60分鐘)
                    self.park_road[table][road][timestamp][2] = obj[0] / (cnt * 60.0) * 100
                    # Total car = New car + Old car
                    self.park_road[table][road][timestamp][5] = obj[3] + obj[4]
                    # Supply = Total car / Cnt
                    self.park_road[table][road][timestamp][7] = self.park_road[table][road][timestamp][5] / cnt * 100
                    # Turnover = New car / Cnt
                    self.park_road[table][road][timestamp][8] = obj[3] / cnt * 100

                    # 所有欄位四捨五入至小數點後第四位
                    self.park_road[table][road][timestamp] = [round(num, 4) for num in self.park_road[table][road][timestamp]]
                        

    def to_list(self, table):
        hourly_data = pd.concat({k: pd.Series(v) for k, v in self.park_road[table].items()}).reset_index()
        new_col = hourly_data.pop(0)
        new_col = np.array(new_col.tolist()).T
        for i, obj in enumerate(new_col):
            hourly_data[i+2] = obj
        try:
            hourly_data.columns = HOURLY_DATA_COLUMNS
        except ValueError:
            logging.warning('No Data!!')
            raise
            
        cnt_filter = hourly_data['cnt'] == 0
        hourly_data = hourly_data[~cnt_filter]
        
        return hourly_data.values.tolist()