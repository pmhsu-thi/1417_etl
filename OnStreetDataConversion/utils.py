import logging
import pandas as pd
from datetime import time, date, datetime

from sql_conn import get_data_date_range
from args import NAME_TYPES_CONVERSION, HOURLY_TABLES, NAME_TYPE_TO_TABLE

def stop():
    import sys
    sys.exit()

# 獲取資料處理開始結束日期
def get_date_period(start, end, period_type):
    if period_type == 'DateRange':
        try:
            start_date = datetime.strptime(start, "%Y-%m-%d").date()
            end_date = datetime.strptime(end, "%Y-%m-%d").date()
        except:
            logging.error('Date format is wrong')
            stop()
    else:
        start_date, end_date = get_data_date_range()

    logging.info(f"Data range from {start_date} to {end_date}")
    return start_date, end_date

# 正向表列收費時段
def positive_list(charge_dict):
    '''
    charging_period (各種收費類型的收費時間)
    {
        charging type :{
            week_no
            0 (星期一) :[9:00, 10:00, ..., 17:00], // 收費時間 9:00~17:00
            ...,
            6 (星期日) :[], // 不收費
            7 (國定假日) :[]
        }
    }
    '''
    total_timestamp = list(map(lambda x: time(x, 0), range(24)))
    charging_period = {}
    for c_type in charge_dict:
        charging_period[c_type] = {}
        for week_no in charge_dict[c_type]:
            charging_period[c_type][week_no] = []
            for period in charge_dict[c_type][week_no]['period']:
                if period[0] == '-1':
                    break
                for timestamp in total_timestamp:
                    if period[0] <= timestamp <= period[1]:
                        charging_period[c_type][week_no].append(timestamp)
                        continue
                    if timestamp > period[1]:
                        break

    return charging_period

def positive_list_union(charging_period):
    '''
    road_charging (各路段中，所有收費類型收費時間的聯集)
    {
        road :{
            week_no
            0~7 :[]
        }
    }
    '''
    road_charging = {}
    for c_type in charging_period:
        road = c_type[:3]
        if road not in road_charging.keys():
            road_charging[road] = {
                0:[],
                1:[],
                2:[],
                3:[],
                4:[],
                5:[],
                6:[],
                7:[]
            }
        for week_no in charging_period[c_type]:
            for time_stamp in charging_period[c_type][week_no]:
                if time_stamp not in road_charging[road][week_no]:
                    road_charging[road][week_no].append(time_stamp)
                    
    return road_charging

# 收費格位數表
def calc_cnt(road_charging, charging_period, grid_static, grid_types, grid_name_types):
    '''
    road_cnt (各種車格類型，在收費時段有收費的格位數)
    {
        hourly_data_table:{    // 'large_vehicle', 'special_vehicle', 'truck', 'vehicle'
            road :{
            week_no
            0~7 :{
                charging hours  // 收費時間 9:00~17:00
                9:00 : charging cnt at 9:00, (收費格位數) 
                10:00 : charging cnt at 10:00,
                ...,
                17:00 : charging cnt at 17:00
            }
        }
        }
    }
    '''
    road_cnt = {}
    # 建立 road_cnt 結構
    for table in HOURLY_TABLES:
        road_cnt[table] = {}
        for road in road_charging:
            road_cnt[table][road] = {
                0:{},
                1:{},
                2:{},
                3:{},
                4:{},
                5:{},
                6:{},
                7:{},
            }
            for week_no in road_charging[road]:
                for timestamp in road_charging[road][week_no]:
                    road_cnt[table][road][week_no][timestamp] = 0

    # 遍歷所有車格收費時段，統計各個時段中有多少車格收費
    for grid in grid_static:
        c_type = grid_types[grid]
        road = grid[:3]
        name_type_table = NAME_TYPE_TO_TABLE[grid_name_types[grid]]
        for week_no in charging_period[c_type]:
            for timestamp in charging_period[c_type][week_no]:
                road_cnt[name_type_table][road][week_no][timestamp] += 1
                
    return road_cnt

# 各收費類型收費時段及小時數查詢表
def charge_to_dict(charge_period):
    '''
    charge_dict (各收費類型收費時段及收費總時數)
    {
        charging type:{
            week_no
            0~7 :{
                period : [9:00, 17:00], // 收費時間 9:00~17:00
                hrs : 總收費時數 (當天收費時數 * 格位數)
            } 
        }
    }
    '''
    charge_dict = {}
    to_time = lambda x: x if int(x) < 0 else time(int(x[:2]), int(x[2:]))
    for row in charge_period:
        c_type = row[0]
        week_no = row[1]
        if c_type not in charge_dict.keys():
            charge_dict[c_type] = {
                0:{'period':[], 'hrs':0},
                1:{'period':[], 'hrs':0},
                2:{'period':[], 'hrs':0},
                3:{'period':[], 'hrs':0},
                4:{'period':[], 'hrs':0},
                5:{'period':[], 'hrs':0},
                6:{'period':[], 'hrs':0},
                7:{'period':[], 'hrs':0}
            }
        charge_dict[c_type][week_no]['period'].append([to_time(row[2]), to_time(row[3])])
        charge_dict[c_type][week_no]['hrs'] += row[4]

    for c_type in charge_dict:
        for week_no in range(8):
            charge_dict[c_type][week_no]['period'] = sorted(charge_dict[c_type][week_no]['period'], key=lambda x: (x[0]))
    return charge_dict

# 各格位收費類型對照表
def grid_type_collation(charge_type):
    '''
    grid_types (各格位收費類型)
    {
        grid : charging type
    }
    '''
    grid_types = {}
    for row in charge_type:
        grid = row[0]
        if grid not in grid_types.keys():
            grid_types[grid] = row[1]
    return grid_types

# 各格位車格類型對照表
def grid_name_type_collation(grid_static):
    '''
    grid_name_types (車格類型)
    {
        grid : name type
    }
    '''
    grid_name_types = {}
    for row in grid_static:
        grid = row[0]
        if grid not in grid_name_types.keys():
            try:
                grid_name_types[grid] = NAME_TYPES_CONVERSION[row[2]]
            except:
                logging.warning(f"{row[1]} not in NAME_TYPES_CONPARISON")
    return grid_name_types

def get_time_range(origin_time, target_time):
    sta_time_list = []
    time_range = pd.date_range(origin_time, target_time, freq='1H').delete(-1)
    for _, obj in enumerate(time_range):
        sta_time_list.append(datetime.strptime(obj.strftime('%Y-%m-%d-%H-%M'), "%Y-%m-%d-%H-%M"))
    return sta_time_list

# 判斷收費日期
def get_week_no(date, holidays):
    if date in holidays:
        return 7
    else:
        return date.weekday()

# 限制開單時間於收費時段中
def pan_time(*args, **kwargs):
    # 該日未收費
    if kwargs['time_start'] == '-1' or kwargs['time_end'] == '-1':
        return None, None
    # 開單的開始結束時間應在同一天內
    benchmark_start = datetime(
        kwargs['start'].year, kwargs['start'].month, kwargs['start'].day, kwargs['time_start'].hour, kwargs['time_start'].minute
    )
    benchmark_end = datetime(
        kwargs['start'].year, kwargs['start'].month, kwargs['start'].day, kwargs['time_end'].hour, kwargs['time_end'].minute
    )
    if kwargs['end'].date() > kwargs['start'].date():
        kwargs['end'] = datetime(
            kwargs['start'].year, kwargs['start'].month, kwargs['start'].day, benchmark_end.hour-1, 59
        )
    # 未超出收費時段
    if (
        (kwargs['start'] >= benchmark_start) and
        (kwargs['end'] < benchmark_end)
    ):
        return kwargs['start'], kwargs['end']

    # 開單時間完全在收費時段外
    if kwargs['end'] <= benchmark_start:
        logging.info(f"grid {kwargs['grid']} bill at {kwargs['end']} to {kwargs['end']} out of charging start {benchmark_start}")
        return None, None
        
    # 該日有收費，但超出收費時段
    if kwargs['end'] >= benchmark_end:
        if kwargs['end'] > benchmark_end:
            logging.debug(f"grid {kwargs['grid']} at {kwargs['end']} out of charging period(end) {benchmark_end}")
        kwargs['end'] = benchmark_end.replace(hour=benchmark_end.hour-1, minute=59, second=59)
    if kwargs['start'] < benchmark_start:
        logging.debug(f"grid {kwargs['grid']} at {kwargs['start']} out of charging period(start) {benchmark_start}")
        kwargs['start'] = benchmark_start.replace(hour=benchmark_start.hour, minute=0, second=0)
    return kwargs['start'], kwargs['end']

# Insert parking event between timestamp
def add_event(timestamp, tmp_dict, tmp_sorted, grid, grid_index):
    grid_index_stop = 0
    # When the next timestamp time point is not reached, continue to insert the parking event
    while timestamp >= tmp_dict[grid][grid_index][0]:
        tmp_sorted[grid].append(tmp_dict[grid][grid_index])
        # Break the loop when all events of this grid are processed
        if grid_index == len(tmp_dict[grid])-1:
            grid_index_stop = 1
            break
        # Break the loop when date change
        if tmp_dict[grid][grid_index][0].date() != tmp_dict[grid][grid_index+1][0].date():
            grid_index += 1
            break
        grid_index += 1
    return grid_index_stop, grid_index

class GridState:
    def __init__(self):
        # 分時時段內狀態
        self.state = {
            'reset':0,
            'number':0,
            'used':0,
            'new':0,
            'old':0,
            'leave':0,
            'onsite':0,
            'successful':0,
            'metered':0
        }

    def reset_state(self):
        self.__init__()

    def car_enter(self, delta_time):
        self.state['used'] += round(delta_time.seconds * self.state['number'] / 60.0, 2)
        self.state['new'] += 1
        self.state['number'] += 1

    def car_leave(self, delta_time):
        self.state['used'] += round(delta_time.seconds * self.state['number'] / 60.0, 2)
        self.state['leave'] += 1
        self.state['number'] -= 1

    def write_data(self, park_grid, grid, time_stamp, delta_time):
        self.state['used'] += round(delta_time.seconds * self.state['number'] / 60.0, 2)
        park_grid.append(
            [grid, time_stamp, self.state['used'], self.state['new'], self.state['old'], self.state['leave']]
        )
        car_cnt = self.state['number']
        self.__init__()
        self.state['old'], self.state['number'] = car_cnt, car_cnt