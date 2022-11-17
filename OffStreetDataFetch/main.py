from datetime import datetime

from sql_conn import PostgreSQL
from dynamic_data import DataFetch

if __name__ == '__main__':
    infotime = datetime.now()
    fetch = DataFetch()
    dynamic_url = 'https://data.ntpc.gov.tw/api/datasets/E09B35A5-A738-48CC-B0F5-570B67AD9C78/csv/file'
    dynamic_data = fetch.dynamic_data(dynamic_url, infotime)

    psql = PostgreSQL()
    psql.insert_dynamic_data(dynamic_data)