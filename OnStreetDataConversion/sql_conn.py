import psycopg2
import logging
from psycopg2 import sql

from args import HOURLY_TABLES, HOURLY_DATA_COLUMNS, DAILY_DATA_COLUMNS

class ConnSQL:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        # conn = psycopg2.connect(
        #   database='postgres',
        #   user='root',
        #   password='thi168168',
        #   host='220.130.185.36',
        #   port='5432')
        conn = psycopg2.connect(
          database='postgres',
          user='thi',
          password='thi',
          host='172.18.16.198',
          port='5432')
        try:
            result = self.func(*args, conn=conn, **kwargs)
        except ValueError as err:
            conn.rollback()
            logging.error(f'Unexpected {err}, {type(err)}')
            raise
        else:
            conn.commit()
            logging.info(f'>> SQL Successful!!')
        finally:
            conn.close()
        return result

@ConnSQL
def __get_data_tmp(*args, conn, **kwargs):
    result = {}
    with conn.cursor() as cur:
        cols = ["(official_id||'-'||lac_code)as lac_code", "(infodate ||' '||substring(start_time from 1 for 2)||':'||substring(start_time from 3 for 2))::timestamp as start_time",
                "((infodate ||' '||substring(start_time from 1 for 2)||':'||substring(start_time from 3 for 2))::timestamp)+ interval  '1 hour'*park_time  as end_time",
                "park_time", "amount"]
        query = sql.SQL("SELECT {col} FROM {table} WHERE (infodate ||' '||substring(start_time from 1 for 2)||':'||substring(start_time from 3 for 2))::timestamp BETWEEN %s AND %s ;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('public.on_street_dynamic')
        )
        cur.execute(query, [args[0], args[1]])
        result['ParkingBill'] = cur.fetchall()

    with conn.cursor() as cur:
        cols=['grid_id', 'charge_type', 'name_type']
        query = sql.SQL("SELECT {col} FROM {table} ;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('public.grid_charge_type_statics')
        )
        cur.execute(query)
        result['GridStatic'] = cur.fetchall()

    with conn.cursor() as cur:
        query = sql.SQL("SELECT {col} FROM {table} ;").format(
            col=sql.SQL('DISTINCT(road_id)'),
            table=sql.SQL('public.on_street_static')
        )
        cur.execute(query)
        result['RoadStatic'] = cur.fetchall()

    with conn.cursor() as cur:
        cols = ['charge_type', 'week_no', 'start_time', 'end_time', 'hrs']
        query = sql.SQL("SELECT {col} FROM {table} ;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('public.road_charge_time_statics')
        )
        cur.execute(query)
        result['ChargePeriod'] = cur.fetchall()

    with conn.cursor() as cur:
        cols = ['grid_id', 'charge_type']
        query = sql.SQL("SELECT {col} FROM {table} ;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('public.grid_charge_type_statics')
        )
        cur.execute(query)
        result['ChargeType'] = cur.fetchall()

    with conn.cursor() as cur:
        query = sql.SQL("SELECT {col} FROM {table} WHERE is_national = 1 AND date_col BETWEEN %s AND %s;").format(
            col=sql.SQL('date_col'),
            table=sql.SQL('public.holidays')
        )
        logging.debug(query.as_string(conn))
        cur.execute(query, [args[0].strftime("%Y-%m-%d"), args[1].strftime("%Y-%m-%d")])
        result['Holiday'] = cur.fetchall()

    return result

# get_data(data_type, start_date, end_date)
def get_data(*args):
    logging.info('|----- Fetch Raw Data -----|')
    tmp = __get_data_tmp(args[0], args[1])
    raw_data = tmp['ParkingBill']
    grid_static = tmp['GridStatic']
    road_static = tmp['RoadStatic']
    charge_period = tmp['ChargePeriod']
    charge_type = tmp['ChargeType']
    holidays = tmp['Holiday']
    return raw_data, grid_static, road_static, charge_period, charge_type, holidays


@ConnSQL
def __insert_data(*args, conn, **kwargs):
    with conn.cursor() as cur:
        query = sql.SQL("INSERT INTO {table} ({col}) VALUES ({value}) ON CONFLICT ({pkey}) DO NOTHING;").format(
            table=sql.SQL(kwargs['table']),
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in kwargs['cols']]),
            value=sql.SQL(',').join([sql.SQL('%s') for _ in range(len(kwargs['cols']))]),
            pkey=sql.SQL(',').join([sql.SQL(col_name) for col_name in kwargs['pkey']])
        )
        logging.debug(query.as_string(conn))
        for row in args[0]:
            logging.debug(row)
            cur.execute(query, tuple(row))

def insert_hourly_data(data):
    
    primary_key = ['officialid', 'infotime']
    
    for table in HOURLY_TABLES:
        table_name = f'public.on_street_dynamic_hour_{table}'
        insert_data = data[table]
        logging.info(f'|----- Insert Hourly {table} Data -----|')
        __insert_data(
            insert_data,
            table=table_name,
            cols=HOURLY_DATA_COLUMNS,
            pkey=primary_key
        )

def insert_daily_data(data):
    logging.info('|----- Insert Daily Data -----|')
    primary_key = ['officialid', 'infodate', 'name_type']

    __insert_data(
        data,
        table='public.on_street_dynamic_days',
        cols=DAILY_DATA_COLUMNS,
        pkey=primary_key
    )

def main():
    raw_data, grid_static, road_static, charge_period, charge_type = get_data('2022-03-29', '2022-03-31')
    for row in raw_data:
        if row[0] == '305-44':
            print(row)

if __name__ == '__main__':
    main()