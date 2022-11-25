import psycopg2
import logging
from psycopg2 import sql

class ConnTHISQL:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        conn = psycopg2.connect(
          database='postgres',
          user='root',
          password='thi168168',
          host='220.130.185.36',
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

class Conn1417SQL:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
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

@ConnTHISQL
def get_off_street_data(*args, conn, **kwargs):
    logging.info('|----- Get RealTime OffStreet Data From THI DataBase -----|')
    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM {table} WHERE infotime BETWEEN %s AND %s;").format(
            table=sql.SQL('off_street_dynamic')
        )
        cur.execute(query, [args[0], args[1]])
        res = cur.fetchall()
    return res

@ConnTHISQL
def get_off_street_hours_data(*args, conn, **kwargs):
    logging.info('|----- Get Hourly OffStreet Data From THI DataBase -----|')
    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM {table} WHERE infotime BETWEEN %s AND %s;").format(
            table=sql.SQL('off_street_dynamic_hours')
        )
        cur.execute(query, [args[0], args[1]])
        res = cur.fetchall()
    return res

@Conn1417SQL
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

def insert_off_street_data(data):
    logging.info('|----- Insert RealTime OffStreet Data -----|')
    primary_key = 'id infotime'.split(' ')
    columns = 'id availablecar infotime'.split(' ')
    __insert_data(
        data,
        table='off_street_dynamic',
        cols=columns,
        pkey=primary_key
    )

def insert_off_street_hours_data(data):
    logging.info('|----- Insert Hourly OffStreet Data -----|')
    primary_key = 'id infotime'.split(' ')
    columns = 'id infotime type totalstay usage totalcar supply'.split(' ')
    __insert_data(
        data,
        table='off_street_dynamic_hours',
        cols=columns,
        pkey=primary_key
    )

@ConnTHISQL
def get_multi_data(*args, conn, **kwargs):
    logging.info('|----- Get Multi Data From THI DataBase -----|')
    res = {}
    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM multi_total_supply;")
        cur.execute(query)
        res['supply'] = cur.fetchall()
        query = sql.SQL("SELECT * FROM multi_total_totalcar;")
        cur.execute(query)
        res['totalcar'] = cur.fetchall()
        query = sql.SQL("SELECT * FROM multi_total_usage;")
        cur.execute(query)
        res['usage'] = cur.fetchall()
    return res

TABLE_COLUMNS = [
    'infotime', 'district', 'car_type', 'hr_0', 'hr_1', 'hr_2', 'hr_3', 'hr_4', 'hr_5',
    'hr_6', 'hr_7', 'hr_8', 'hr_9', 'hr_10', 'hr_11', 'hr_12', 'hr_13', 'hr_14', 'hr_15',
    'hr_16', 'hr_17', 'hr_18', 'hr_19', 'hr_20', 'hr_21', 'hr_22', 'hr_23'
]

def insert_multi_data(data):
    for table in data:
        logging.info(f'|----- Insert multi total {table} Data -----|')
        table_name = f'multi_total_{table}'

        __insert_data(
            data[table],
            table=table_name,
            cols=TABLE_COLUMNS,
            pkey=['infotime', 'district', 'car_type']
        )