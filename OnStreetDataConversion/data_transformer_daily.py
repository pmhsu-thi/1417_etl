import logging
from utils import get_week_no
from args import DAILY_DATA_COLUMNS

class DataTransformerDaily:
    def to_daily_road_data(self):
        '''
        daily_dict
        {
            pkey = 'road_id # infodate # name_type'
            pkey : [ DAILY_DATA_COLUMNS 中減去 'officialid', 'infodate', 'name_type' 後剩餘欄位]
        }
        '''
        self.data_dict = {}
        column_number = len(DAILY_DATA_COLUMNS) - 3
        
        for row in self.raw_data:
            road = row[0][:3]
            # name_type 中文轉數字編號
            name_type = self.name_type_convert(row[0])
            if name_type is None:
                continue
            # 組成唯一鍵 (路段ID # 開單日期 # name_type)
            week_no = get_week_no(row[1].date(), self.holidays, self.workdays)
            self.debug_check_cnt(road, week_no, name_type, row[1].date())
            if self.skip is True:
                continue

            pk = f'{road}#{row[1].date()}#{name_type}'
            if pk not in self.data_dict.keys():
                self.data_dict[pk] = [0 for _ in range(column_number)]
                # 總收費時數
                self.data_dict[pk][-2] = self.mean_cnt[road][week_no][name_type][0]
                # 收費時段平均格位數 = 收費總時數 / 該路段最高收費時數
                self.data_dict[pk][-1] = self.mean_cnt[road][week_no][name_type][0] / self.mean_cnt[road][week_no][name_type][1]
            #  該類別單數
            self.data_dict[pk][0] += 1
            # 停車時間
            self.data_dict[pk][1] += row[3]
            # 開單總金額
            self.data_dict[pk][2] += row[4]

    def split_dict(self):
        for i, pk in enumerate(self.data_dict):
            self.road_dynamic.append(pk.split('#')) 
            self.road_dynamic[i].extend(self.data_dict[pk])

    def calc_mean_cnt(self):
        '''
        maen_cnt
        {
            road :{
                week_no
                0~7 :[
                    [總收費時數, 該路段最高收費時數 (大型車停車格)],\n
                    [總收費時數, 該路段最高收費時數 (汽車身心障礙專用)],\n
                    [總收費時數, 該路段最高收費時數 (裝卸貨專用停車位)],\n
                    [總收費時數, 該路段最高收費時數 (汽車停車位)],\n
                    [總收費時數, 該路段最高收費時數 (家長接送區)],\n
                    [總收費時數, 該路段最高收費時數 (時段性禁停停車位)]\n
                ],...
            }
        }
        '''
        self.mean_cnt = {}
        for grid in self.grid_static:
            road = grid[:3]
            name_type = self.grid_name_types[grid]
            c_type = self.grid_types[grid]
            # 建立結構
            if road not in self.mean_cnt.keys():
                self.mean_cnt[road] = {
                    0:[[0, 0] for _ in range(6)],
                    1:[[0, 0] for _ in range(6)],
                    2:[[0, 0] for _ in range(6)],
                    3:[[0, 0] for _ in range(6)],
                    4:[[0, 0] for _ in range(6)],
                    5:[[0, 0] for _ in range(6)],
                    6:[[0, 0] for _ in range(6)],
                    7:[[0, 0] for _ in range(6)]
                }

            # 計算總收費時數
            for week_no in self.charge_dict[c_type]:
                self.mean_cnt[road][week_no][name_type][0] += self.charge_dict[c_type][week_no]['hrs']
                # 紀錄目前單一格位最高收費時數
                if self.charge_dict[c_type][week_no]['hrs'] > self.mean_cnt[road][week_no][name_type][1]:
                    self.mean_cnt[road][week_no][name_type][1] = self.charge_dict[c_type][week_no]['hrs']
        
    # name_type 中文轉為數字編號
    def name_type_convert(self, grid):
        if grid in self.grid_name_types.keys():
            return self.grid_name_types[grid]
        if grid in self.except_grid:
            return 
        else:
            self.except_grid.append(grid)
            return

    def debug_check_cnt(self, road, week_no, name_type, bill_date):
        if not self.mean_cnt[road][week_no][name_type][1]:
            logging.warning('>> Out of charging period!!')
            logging.warning(f'Road : {road}')
            logging.warning(f'Date : {bill_date}')
            logging.warning(f'Name_type : {name_type}')
            self.skip = True
            return
        self.skip = False