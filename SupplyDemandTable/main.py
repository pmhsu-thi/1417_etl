import logging

from read_csv import read_csv
from sql_conn import insert_single_system_data, insert_multi_system_data
from data_process import DataProcess, data_to_dict
from args import ARGS

def main():
    dp = DataProcess()
    for year in ARGS['year']:
        for calendar in ARGS['calendar']:
            for veh_type in ARGS['vehicle_types']:
                logging.info(f'>> {year}/{calendar}/{veh_type}')
                raw_data = read_csv(year, calendar, veh_type)
                raw_dict = data_to_dict(raw_data)
                for system_type in ARGS['system_type']:
                    if system_type == 'single':
                        result = dp.single_system(year, calendar, veh_type, raw_dict)
                        insert_single_system_data(result)
                        continue
                    if system_type == 'multi':
                        result = dp.multi_system(year, calendar, veh_type, raw_dict)
                        insert_multi_system_data(result)
                        continue
                    logging.error(f'>> 輸入的 system_type: {system_type} 有誤!!')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()