import psycopg2
import logging
from psycopg2 import sql

from args import TABLE_COLUMNS

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

def insert_single_system_data(data):
    for table in data:
        logging.info(f'|----- Insert single {table} Data -----|')
        table_name = f'public.sd_{table}'

        __insert_data(
            data[table],
            table=table_name,
            cols=TABLE_COLUMNS,
            pkey=['year', 'area', 'car_type', 'is_workday']
        )

def insert_multi_system_data(data):
    for table in data:
        logging.info(f'|----- Insert multi {table} Data -----|')
        table_name = f'public.multi_sd_{table}'

        __insert_data(
            data[table],
            table=table_name,
            cols=TABLE_COLUMNS,
            pkey=['year', 'area', 'car_type', 'is_workday']
        )