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
    logging.info('|----- Get Data From THI DataBase -----|')
    with conn.cursor() as cur:
        query = sql.SQL("SELECT * FROM {table} WHERE infotime BETWEEN %s AND %s;").format(
            table=sql.SQL('off_street_dynamic')
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
    logging.info('|----- Insert Off_Street Data -----|')
    primary_key = 'id infotime'.split(' ')
    columns = 'id availablecar infotime'.split(' ')
    __insert_data(
        data,
        table='off_street_dynamic',
        cols=columns,
        pkey=primary_key
    )