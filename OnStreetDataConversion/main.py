import logging

from data_process import DataProcess
from sql_conn import insert_hourly_data, insert_daily_data
from utils import get_date_period
from args import date_type, start, end

def main():
    logging.basicConfig(level=logging.INFO)
    # 確定資料處理日期範圍
    start_date, end_date = get_date_period(start, end, date_type)
    # 處理計算為 分時 及 分日 停車資料，分時資料是資料庫表名(車格停車類型)做為 key 的 dict
    dp = DataProcess(start_date, end_date)
    hourly_data, daily_data = dp.main_data_process()
    # 寫入資料庫
    insert_daily_data(daily_data)
    insert_hourly_data(hourly_data)


if __name__ == '__main__':
    main()