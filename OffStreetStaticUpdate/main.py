import csv
import math
import psycopg2
import urllib.request
from datetime import datetime

# 連線 PostgreSQL
class PostgreSQL:
    def __init__(self):
        return
        
    def sql_conn(self):
        # self.conn = psycopg2.connect(
        #   database='postgres',
        #   user='root',
        #   password='thi168168',
        #   host='220.130.185.36',
        #   port='5432')
        self.conn = psycopg2.connect(
          database='postgres',
          user='thi',
          password='thi',
          host='172.18.16.198',
          port='5432')

    def __sql_fetch(self, sql):
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def __sql_execute(self, sql, strings):
        self.cur = self.conn.cursor()
        self.cur.execute(sql, strings)

#     抓取最新靜態資料表 (需保留舊版本靜態資料表時使用)  
#     def fetch_latest_data(self):
#         self.sql_conn()
#         try:
#             sql = "SELECT max(update_date) FROM public.off_street_static;"
#             latest_date = self.__sql_fetch(sql)
#             latest_date = latest_date[0][0]
#             print(latest_date)
            
#             sql = f"SELECT * FROM public.off_street_static WHERE update_date = '{latest_date}';"
#             latest_data = self.__sql_fetch(sql)
#         except ValueError as err:
#             print(f"Unexpected {err}, {type(err)} while SELECT latest data")
#         else:
#             print('latest data select completed')
#         finally:
#             self.conn.close()
            
#         return latest_data

    def truncate_table(self):
        self.sql_conn()
        cur = self.conn.cursor()
        cur.execute('TRUNCATE TABLE public.off_street_static;')
        self.conn.commit()
        
    def insert_static_data(self, data):
        self.sql_conn()
        try:
            table = 'public.off_street_static'
            insert_obj = 'id, area, name, type, summary, address, tel, payex, servicetime,\
                longitude, latitude, totalcar, totalmotor, totalbike, update_date'
            values = '%s,'*len(insert_obj.split(','))
            values = values[:-1]
            unique_key = 'id'
            sql = f"INSERT INTO {table} ({insert_obj}) VALUES\
                ({values}) ON CONFLICT ({unique_key}) DO NOTHING;"
            for i, row in enumerate(data):
                print(i/len(data), end='\r')
                self.__sql_execute(sql, tuple(row))
            self.conn.commit()
        except ValueError as err:
            print(f"Unexpected {err}, {type(err)} while INSERT static Data")
        else:
            print('Static data insert completed')
        finally:
            self.conn.close()

# 座標轉換
def twd97_to_latlng(x, y):
    a = 6378137
    b = 6356752.314245
    long_0 = 121 * math.pi / 180.0
    k0 = 0.9999
    dx = 250000
    dy = 0
    
    e = pow((1-pow(b, 2)/pow(a,2)), 0.5)
    
    x -= dx
    y -= dy
    
    m = y / k0
    
    mu = m / (a * (1 - pow(e, 2) / 4 - 3 * pow(e, 4) / 64 - 5 * pow(e, 6) / 256))
    e1 = (1.0 - pow((1 - pow(e, 2)), 0.5)) / (1.0 +pow((1.0 - pow(e, 2)), 0.5))
    
    j1 = 3 * e1 / 2 - 27 * pow(e1, 3) / 32
    j2 = 21 * pow(e1, 2) / 16 - 55 * pow(e1, 4) / 32
    j3 = 151 * pow(e1, 3) / 96
    j4 = 1097 * pow(e1, 4) / 512
    
    fp = mu + j1 * math.sin(2*mu) + j2 * math.sin(4*mu) + j3 * math.sin(6*mu) + j4 * math.sin(8*mu)
    
    e2 = pow((e*a/b), 2)
    c1 = pow(e2*math.cos(fp), 2)
    t1 = pow(math.tan(fp), 2)
    r1 = a * (1 - pow(e, 2)) / pow((1-math.pow(e, 2)*pow(math.sin(fp), 2)), (3 / 2))
    n1 = a / pow((1-pow(e, 2)*pow(math.sin(fp), 2)), 0.5)
    d = x / (n1 * k0)
    
    q1 = n1* math.tan(fp) / r1
    q2 = pow(d,2) / 2
    q3 = (5 + 3*t1 + 10*c1 - 4*pow(c1, 2) - 9*e2) * pow(d,4) / 24
    q4 = (61 + 90*t1 + 298*c1 + 45*pow(t1, 2) - 3*pow(c1, 2) - 252*e2) * pow(d,6) / 720
    lat = fp - q1 * (q2 - q3 + q4)
    
    q5 = (1 + 2*t1 + c1) * pow(d, 3) / 6
    q6 = (5 - 2*c1 + 28*t1 - 3*pow(c1, 2) + 8*e2 + 24*pow(t1, 2)) * pow(d, 5) / 120
    lng = long_0 + (d - q5 + q6) / math.cos(fp)
    
    lat = (lat * 180) / math.pi
    lng = (lng * 180) / math.pi
    
    return lng, lat

# API抓取資料
def fetch_data(url):
    webpage = urllib.request.urlopen(url)
    rows = csv.reader(webpage.read().decode('utf-8').splitlines())
    next(rows, None)
    static_data = []
    for row in rows:
        static_data.append(row)
    
    return static_data

# 資料整理
def data_process(static_data, update_date):
    for i, item in enumerate(static_data):
        x = float(item[9])
        y = float(item[10])
        lng, lat = twd97_to_latlng(x, y)
        static_data[i][9], static_data[i][10] = lng, lat
        static_data[i][0].zfill(6)
        static_data[i][11], static_data[i][12] = int(static_data[i][11]), int(static_data[i][12])
        try:
            static_data[i][13] = int(item[13])
        except:
            static_data[i][13] = 0
        static_data[i].append(update_date)

def to_list(NestedTuple):
    return list(map(to_list, NestedTuple)) if isinstance(NestedTuple, (list, tuple)) else NestedTuple


if __name__ == '__main__':
    infotime = datetime.now()
    update_date = infotime.date()

    url = 'https://data.ntpc.gov.tw/api/datasets/B1464EF0-9C7C-4A6F-ABF7-6BDF32847E68/csv/file'
    static_data = fetch_data(url)

    data_process(static_data, update_date)

    ps = PostgreSQL()
    ps.truncate_table()
    ps.insert_static_data(static_data)

    