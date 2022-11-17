import os
import sys
import csv
import logging
import urllib.request
import pandas as pd
from datetime import datetime

from sql_conn import fetch_road_district

class GetData:
    def fetch_opendata(self):
        logging.info('|----- Fetch Open Data -----|')
        url = 'https://data.ntpc.gov.tw/api/datasets/54A507C4-C038-41B5-BF60-BBECB9D052C6/csv'
        webpage = urllib.request.urlopen(url)
        rows = csv.reader(webpage.read().decode('utf-8').splitlines())
        next(rows, None)
        static_data = []
        for row in rows:
            static_data.append(row)
        return static_data

    def get_path(self):
        path = './RoadNameData/'
        if len(os.listdir(path)) != 1:
            logging.warning(f'>> There are {len(os.listdir(path))} file in folder')
        file = os.listdir(path)[-1]
        if '.txt' in file:
            self.file_path = os.path.join(path, file)
            logging.info(f'>> read {file}')
            return
        logging.critical('>> Filename Extension not .txt')
        sys.exit()

    def read_roadnames_data(self):
        logging.info('|----- Read RoadName Data -----|')
        self.get_path()

        with open(self.file_path, encoding='utf-8') as f:
            district_data = []
            for line in f.readlines():
                line = line.strip('\n')
                district_data.append([line[:3], line[4:]])
        return district_data

class DataProcess:
    def district_table():
        data = fetch_road_district()
        road_district = {}
        for row in data:
            if row[0] not in road_district.keys():
                road_district[row[0]] = row[1][:3]
                
        return road_district

    def static_data_process(static, district):
        infodate = datetime.now().date()
        for row in static:
            row[1] = row[1].split('.')[0]
            row[12] = district[row[8]]
            del row[10:12]
            del row[7]
            row.append(infodate)

if __name__ == '__main__':
    gd = GetData()
    data = gd.fetch_opendata()
    data = pd.DataFrame(data)
    print(data)