import logging
from datetime import datetime

from data_transformer_daily import DataTransformerDaily
from data_transformer_hourly import DataTransformerHourly
from sql_conn import get_data
from utils import get_date_period, get_time_range, charge_to_dict, grid_type_collation, grid_name_type_collation
from args import HOURLY_TABLES

class DataProcess(DataTransformerDaily, DataTransformerHourly):
    def __init__(self, start_date, end_date):
        self.start_date, self.end_date = start_date, end_date
        self.time_arr = get_time_range(start_date, end_date)

        # Hourly Data Table
        self.grid_dict = {}
        self.grid_sorted = {}
        self.park_road = {}
        self.hourly_data = {}
        for table in HOURLY_TABLES:
            self.park_road[table] = {}

        # Daily Data Table
        self.road_dynamic = []
        self.except_grid = []

    # 總處理流程
    def main_data_process(self, return_except=False):
        # 從資料庫抓取資料
        self.raw_data, grid_static, road_static, charge_period, charge_type, holidays, workdays = get_data(self.start_date, self.end_date)
        
        # 車格收費 及 停車類型 對照表處理、車格 及 路段 靜態資料表處理、國定假日正向列表
        self.charge_dict = charge_to_dict(charge_period)
        self.grid_types = grid_type_collation(charge_type)
        self.grid_name_types = grid_name_type_collation(grid_static)
        self.grid_static = list(map(lambda x: x[0], grid_static))
        self.road_static = list(map(lambda x: x[0], road_static))
        self.holidays = list(map(lambda x: datetime.strptime(x[0], '%Y-%m-%d').date(), holidays))
        self.workdays = list(map(lambda x: datetime.strptime(x[0], '%Y-%m-%d').date(), workdays))

        # 分日及分時資料處理
        logging.info('|----- Start Data Processing -----|')
        daily_data = self.road_dynamics_daily()
        hourly_data = self.road_dynamics_hourly()
        
        if return_except == False:
            return hourly_data, daily_data
        return hourly_data, daily_data, self.except_grid

    # daily data 分日資料處理流程
    def road_dynamics_daily(self):
        DataTransformerDaily()
        # 建立路段中各類型車格 [ 收費總時數, 最高收費時數 ], 用於 daily_data 的 總收費時數 及 每小時平均格位數
        self.calc_mean_cnt()
        # 使用 road_id # infodate # name_type 為 key 建立 dict, value為 [單數, 時長, 金額, 總收費時數, 每小時平均格位數]
        self.to_daily_road_data()
        # 將 dict 中的 key 拆開並轉成 list
        self.split_dict()

        logging.info(">> Daily Data Processing Completed!!")
        return self.road_dynamic

    # hourly data 分時資料處理流程
    def road_dynamics_hourly(self):
        DataTransformerHourly()
        # 整理收費時段相關靜態資料表格
        self.static_data_process()

        # 原始資料轉為各格位進出事件 dict 並依時間先後排序
        self.raw_data_process()
        # dict 資料中插入分時時間戳, 後續迴圈碰到時間戳時整理紀錄該小時資料
        self.insert_timestamp(self.grid_dict, self.grid_sorted)
        # 格位事件資料 >> 格位分時資料 [Grid_code, Infotime, Usetime, Newcar, Oldcar, Leavecar]
        park_grid = self.to_grid_period_data(self.grid_sorted)
        # 分時格位資料 >> 分時路段資料 (區分出各表格中包含的車格類型)
        self.to_road_period_data(park_grid)
        
        # 各車格類型表格資料型態轉換, dict >> list
        for table in HOURLY_TABLES:
            self.hourly_data[table] = self.to_list(table)

        logging.info(">> Hourly Data Processing Completed!!")
        return self.hourly_data



# 測試數據處理功能是否正常
def main(test_type=None):
    logging.basicConfig(level=logging.DEBUG)
    start_date, end_date = get_date_period('2022-03-29', '2022-03-31', 'DateRange')
    dp = DataProcess(start_date, end_date)
    hourly_data, daily_data = dp.main_data_process()
    if test_type == 'hourly':
        for table in HOURLY_TABLES:
            logging.debug(f"{table} Hourly Data: {hourly_data[table][0:3]}")
        return
    if test_type == 'daily':
        logging.debug(f"Daily Data: {daily_data[0:3]}")
        return
    
    for table in HOURLY_TABLES:
        logging.debug(f"{table} Hourly Data: {hourly_data[table][0:3]}")
    logging.debug(f"Daily Data: {daily_data[0:3]}")

if __name__ == '__main__':
    # 測試默認為 hourly_data, daily_data 全部輸出, 可選 test('hourly') 或 test('daily')
    main('hourly')