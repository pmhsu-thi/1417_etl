import os
import csv
import logging
from sql_conn import get_multi_data, insert_multi_data

def process_type():
    print('Choose Input or Output Data (input or output)')
    mode = input('>> ')
    return mode

def read_csv():
    res = {
        'supply':[],
        'totalcar':[],
        'usage':[]
    }
    path = input('Enter Folder Path: ')
    for table in res:
        file_path = os.path.join(path, f'{table}.csv')
        with open(file_path, newline='') as csvfile:
            rows = csv.reader(csvfile)
            for row in rows:
                res[table].append(row)
        
    return res

def write_csv(data):
    logging.info('|----- Write CSV -----|')
    for table in data:
        logging.info(f'>> multi_total_{table}.csv Output')
        with open(f'{table}.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data[table])

def main():
    mode = process_type()
    if mode == 'output':
        data = get_multi_data()
        write_csv(data)
        return
    if mode == 'input':
        data = read_csv()
        insert_multi_data(data)
        return
    logging.error(f'>> mode: {mode},  Mode Error!!')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()