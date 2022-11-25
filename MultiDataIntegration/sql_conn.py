import psycopg2
import logging
from psycopg2 import sql
from datetime import datetime, timedelta
from args import SQL_ARGS, TABLE_COLUMNS

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
            logging.error(f'>> Unexpected {err}, {type(err)}')
            raise
        else:
            conn.commit()
            logging.info(f'>> SQL Successful!!')
        finally:
            conn.close()
        return result

@ConnSQL
def __fetch_data(*args, conn, **kwargs):
    result = {}

    # 路邊靜態資料表
    with conn.cursor() as cur:
        cols = ['road_id', '\"name\"', 'district']
        query = sql.SQL("SELECT {col} FROM {table};").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('on_street_static')
        )
        cur.execute(query)
        result['Onstreet_Static'] = cur.fetchall()

    # 路邊汽車分時資料表
    with conn.cursor() as cur:
        cols = ['officialid', 'infotime', 'totalcar']
        query = sql.SQL("SELECT {col} FROM {table} WHERE infotime >= %s AND infotime < %s;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('on_street_dynamic_hour_vehicle')
        )
        cur.execute(query, [kwargs['start'], kwargs['end']])
        result['OnStreet_Vehicle'] = cur.fetchall()

    # 路邊特殊汽車分時資料表
    with conn.cursor() as cur:
        cols = ['officialid', 'infotime', 'totalcar']
        query = sql.SQL("SELECT {col} FROM {table} WHERE infotime >= %s AND infotime < %s;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('on_street_dynamic_hour_special_vehicle')
        )
        cur.execute(query, [kwargs['start'], kwargs['end']])
        result['OnStreet_SpecialVehicle'] = cur.fetchall()

    # 路外靜態資料表
    with conn.cursor() as cur:
        cols = ['id', 'area', 'totalcar']
        query = sql.SQL("SELECT {col} FROM {table};").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('off_street_static')
        )
        cur.execute(query)
        result['OffStreet_Static_Vehicle'] = cur.fetchall()

    with conn.cursor() as cur:
        cols = ['parking_id', 'max(disability_car)']
        query = sql.SQL("SELECT {col} FROM {table} GROUP BY {group};").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('off_street_parkings'),
            group=sql.SQL('parking_id')
        )
        cur.execute(query)
        result['OffStreet_Static_Special'] = cur.fetchall()

    # 路外分時資料表
    with conn.cursor() as cur:
        cols = ['id', 'infotime', 'totalcar']
        query = sql.SQL("SELECT {col} FROM {table} WHERE infotime >= %s AND infotime < %s;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('off_street_dynamic_hours')
        )
        cur.execute(query, [kwargs['start'], kwargs['end']])
        result['OffStreet_Dynamic'] = cur.fetchall()

    # 供需調查資料表
    year = int(kwargs['end'][:4]) - 1911
    logging.info(f'>> 供需調查資料使用至 {year} 年')
    with conn.cursor() as cur:
        cols = [
            '\"year\"', 'district', 'car_type', 'sum(hr_0)', 'sum(hr_1)', 'sum(hr_2)', 'sum(hr_3)', 'sum(hr_4)',
            'sum(hr_5)', 'sum(hr_6)', 'sum(hr_7)', 'sum(hr_8)', 'sum(hr_9)', 'sum(hr_10)', 'sum(hr_11)', 'sum(hr_12)',
            'sum(hr_13)', 'sum(hr_14)', 'sum(hr_15)', 'sum(hr_16)', 'sum(hr_17)', 'sum(hr_18)', 'sum(hr_19)', 
            'sum(hr_20)', 'sum(hr_21)', 'sum(hr_22)', 'sum(hr_23)', 'is_workday'
        ]
        query = sql.SQL(
            "SELECT {col} FROM {table} WHERE \"year\" <= {year} AND car_type in (2, 3)\
            GROUP BY \"year\", district, car_type, is_workday;"
        ).format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('multi_sd_supply'),
            year=sql.SQL(str(year))
        )
        cur.execute(query)
        result['Supply'] = cur.fetchall()

    with conn.cursor() as cur:
        cols = [
            '\"year\"', 'district', 'car_type', 'sum(hr_0)', 'sum(hr_1)', 'sum(hr_2)', 'sum(hr_3)', 'sum(hr_4)',
            'sum(hr_5)', 'sum(hr_6)', 'sum(hr_7)', 'sum(hr_8)', 'sum(hr_9)', 'sum(hr_10)', 'sum(hr_11)', 'sum(hr_12)',
            'sum(hr_13)', 'sum(hr_14)', 'sum(hr_15)', 'sum(hr_16)', 'sum(hr_17)', 'sum(hr_18)', 'sum(hr_19)', 
            'sum(hr_20)', 'sum(hr_21)', 'sum(hr_22)', 'sum(hr_23)', 'is_workday'
        ]
        query = sql.SQL(
            "SELECT {col} FROM {table} WHERE \"year\" <= {year} AND car_type in (2, 3)\
            GROUP BY \"year\", district, car_type, is_workday;"
        ).format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('multi_sd_demand'),
            year=sql.SQL(str(year))
        )
        cur.execute(query)
        result['Demand'] = cur.fetchall()

    # 行事曆資料
    with conn.cursor() as cur:
        cols = ['date_col', 'is_workday']
        query = sql.SQL("SELECT {col} FROM {table} WHERE date_col BETWEEN %s and %s;").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in cols]),
            table=sql.SQL('holidays'),
        )
        cur.execute(query, [kwargs['start'], kwargs['end']])
        result['Holiday'] = cur.fetchall()
        
    return result

@ConnSQL
def __get_data_date(*args, conn, **kwargs):
    res = {}
    with conn.cursor() as cur:
        query = sql.SQL("SELECT {col} FROM {table};").format(
            col=sql.SQL('max(infotime)'),
            table=sql.SQL('multi_total_usage')
        )
        cur.execute(query)
        res['start'] = cur.fetchall()

    with conn.cursor() as cur:
        query = sql.SQL("SELECT {col} FROM {table};").format(
            col=sql.SQL('max(infotime)'),
            table=sql.SQL('on_street_dynamic_hour_special_vehicle')
        )
        cur.execute(query)
        res['end'] = cur.fetchall()
    return res

def get_data_date_range() -> str:
    res = __get_data_date()
    start = res['start'][0][0].date() + timedelta(days=1)
    end = res['end'][0][0].date() + timedelta(days=1)
    return start, end

def define_period(period_type):
    logging.info(f'>> Period Type : {period_type}')
    if period_type == 'args':
        return SQL_ARGS['start'], SQL_ARGS['end']
    return get_data_date_range()  

def get_data(period_type):
    start_date, end_date = define_period(period_type)
    res = __fetch_data(start=start_date, end=end_date)
    return res

@ConnSQL
def __truncate_test_table(*args, conn, **kwargs):
    with conn.cursor() as cur:
        query = sql.SQL("TRUNCATE TABLE multi_test_{table};").format(
            table=sql.SQL(kwargs['table'])
        )
        cur.execute(query)

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

def insert_data(data):
    for table in data:
        logging.info(f'|----- Insert multi total {table} Data -----|')
        table_name = f'multi_total_{table}'

        __insert_data(
            data[table],
            table=table_name,
            cols=TABLE_COLUMNS,
            pkey=['infotime', 'district', 'car_type']
        )

def insert_test_data(data):
    for table in data:
        logging.info(f'|----- Truncate multi test {table} -----|')
        __truncate_test_table(table=table)
        
        logging.info(f'|----- Insert multi test {table} Data -----|')
        table_name = f'multi_test_{table}'
        __insert_data(
            data[table],
            table=table_name,
            cols=TABLE_COLUMNS,
            pkey=['infotime', 'district', 'car_type']
        )


def main():
    result = get_data('args')
    for i in range(10):
        print(result['Onstreet_Static'][i])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()