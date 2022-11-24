import csv
import logging
from sql_conn import get_off_street_data, insert_off_street_data

def process_type():
    print('Choose Input or Output Data (input or output)')
    res = input('>> ')
    return res

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
    with open('off_street_dynamic.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

def main():
    mode = process_type()
    if mode == 'input':
        data = read_csv()
        insert_off_street_data(data)

    elif mode == 'output':
        start, end = get_date()
        data = get_off_street_data(start, end)
        write_csv(data)
        logging.info('>> Output Completed !!')

    else:
        logging.error(f'mode: {mode}, Mode error (only input, output)')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()