import csv
import urllib.request

def negative_number_check(data):
    for i, row in enumerate(data):
        ablecar = int(row[1])
        if ablecar < 0 and ablecar != -9:
            print(f'row {i} avail able car change from {data[i][1]} to -9')
            data[i][1] = '-9'

class DataFetch:
    def __init__(self):
        return

    def fetch_data(self, url):
        webpage = urllib.request.urlopen(url)
        self.rows = csv.reader(webpage.read().decode('utf-8').splitlines())
        next(self.rows, None)

    def static_data(self, url):
        self.fetch_data(url)
        static_data = []
        for row in self.rows:
            static_data.append(row)
        return static_data

    def dynamic_data(self, url, infotime):
        self.fetch_data(url)
        dynamic_data = []
        for row in self.rows:
            dynamic_data.append([row[0], row[1], infotime])
        negative_number_check(dynamic_data)
        return dynamic_data