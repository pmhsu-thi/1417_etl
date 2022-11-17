import logging
from datetime import datetime

from sql_conn import fetch_data, insert_hour_data
from data_conversion import DataConversion

def main():
    logging.basicConfig(level=logging.INFO)
    now = datetime.now()
    start_time = f'{now.year}-{now.month}-{now.day} {now.hour-1}:00:00'
    end_time = f'{now.year}-{now.month}-{now.day} {now.hour}:00:00'

    dynamic_data, static_data = fetch_data(start_time, end_time)

    dc = DataConversion(static_data, dynamic_data)
    dc.calc_total_cnt()
    dc.calc_data_hrs()
    dynamic_hrs = dc.data_process()

    insert_hour_data(dynamic_hrs)

def test(row_numbers):
    logging.basicConfig(level=logging.DEBUG)
    start_time = '2022-03-01 00:00:00'
    end_time = '2022-03-01 12:00:00'

    dynamics, statics = fetch_data(start_time, end_time)
    dc = DataConversion(statics, dynamics)
    dc.calc_total_cnt()
    dc.calc_data_hrs()
    dynamic_data = dc.data_process()
    for i in range(row_numbers):
        logging.debug(dynamic_data[i])

if __name__ == '__main__':
    main()