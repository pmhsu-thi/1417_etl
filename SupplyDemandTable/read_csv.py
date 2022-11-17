import os
import sys
import csv
import logging

from args import ARGS, DATA_PROCESS

def read_csv(year, calendar, veh_type):
    raw_data = []
    path = os.path.join(ARGS['path'], year, calendar, veh_type)
    for file in os.listdir(path):
        file = os.path.join(path, file)
        with open(file, newline='') as csvfile:
            rows = csv.reader(csvfile, delimiter=',')
            next(csvfile)
            for row in rows:
                raw_data_process(row, veh_type)
                standardized_data(row)
                not_number_filter(row)
                raw_data.append(row)    
    return raw_data


def raw_data_process(row, veh_type):
    if veh_type in DATA_PROCESS['del_to4']:
        del row[:4]
    if veh_type in DATA_PROCESS['del_last']:
        del row[-1]

def standardized_data(row):
    # time_sharing
    if len(row[0]) == 1:
        row[0] = f'0{row[0][0]}'
    # area
    if len(row[1]) == 7:
        row[1] = f'0{row[1]}'

    if len(row[0]) == 2 and len(row[1]) == 8:
        return
    else:
        logging.critical('Format Error !!')
        logging.critical(row)
        sys.exit()


def not_number_filter(row):
    for i, item in enumerate(row):
        if i < 2:
            continue
        
        try:
            row[i] = int(item)
        except:
            if item == '1.5':
                row[i] = 1
            else:
                logging.warning(row)
                row[i] = -99


def main():
    year = ARGS['year'][0]
    calendar = ARGS['calendar'][1]
    veh_type = ARGS['vehicle_types'][2]
    data = read_csv(year, calendar, veh_type)
    print(f'{year}/{calendar}/{veh_type}')
    for i in range(10):
        print(data[i])

if __name__ == '__main__':
    main()