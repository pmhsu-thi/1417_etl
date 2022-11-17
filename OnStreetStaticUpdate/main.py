import logging
from data_process import GetData, DataProcess
from sql_conn import truncate_table, insert_road_names_data, insert_statics_data

def update_roadnames():
    data = GetData.read_roadnames_data()
    truncate_table('public.road_names')
    insert_road_names_data(data)

def update_onstreet_static():
    road_district = DataProcess.district_table()
    static_data = GetData.fetch_opendata()
    DataProcess.static_data_process(static_data, road_district)
    if len(static_data) < 33000:
        logging.critical('>> Open Data Less than 33,000')
    truncate_table('public.on_street_static')
    insert_statics_data(static_data)