import csv
import logging
from sql_conn import get_off_street_data, get_off_street_hours_data, insert_off_street_data, insert_off_street_hours_data

def stop():
    import sys
    sys.exit()

def process_type():
    print('Enter Data Type (realtime or hourly)')
    table = input('>> ')
    print('Choose Input or Output Data (input or output)')
    mode = input('>> ')
    return table, mode

def get_date():
    print('Enter Data Recovery Scope (YYYY-mm-dd)')
    start = input('>> start date: ')
    end = input('>> end date: ')
    logging.info(f'|----- Output CSV infotime Between {start} and {end} -----|')
    return start, end

def read_csv():
    res = []
    path = input('Enter File Path: ')
    with open(path, newline='') as csvfile:
        rows = csv.reader(csvfile)
        for row in rows:
            res.append(row)
    return res

def write_csv(data):
    logging.info('|----- Write CSV -----|')
    with open('off_street_data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

def get_data(start, end, table):
    if table == 'realtime':
        res = get_off_street_data(start, end)
        return res
    if table == 'hourly':
        res = get_off_street_hours_data(start, end)
        return res
    logging.error(f'>> data_type: {table},  Data Type Error!!')
    stop()

def insert_data(data, table):
    if table == 'realtime':
        insert_off_street_data(data)
        return
    if table == 'hourly':
        insert_off_street_hours_data(data)
        return
    logging.error(f'>> data_type: {table},  Data Type Error!!')
    stop()

def main():
    table, mode = process_type()
    if mode == 'input':
        data = read_csv()
        insert_data(data, table)
        return
    if mode == 'output':
        start, end = get_date()
        data = get_data(start, end, table)
        write_csv(data)
        logging.info('>> Output Completed !!')
        return

    logging.error(f'mode: {mode}, Mode error (only input, output)')



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()