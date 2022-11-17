import logging
import psycopg2
from psycopg2 import sql

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
def __fetch_data(*args, conn, **kwargs):
    if 'cond' not in kwargs.keys():
        kwargs['cond'] = ''
    
    with conn.cursor() as cur:        
        query = sql.SQL("SELECT {col} FROM {table} {condition};").format(
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in kwargs['cols']]),
            table=sql.SQL(kwargs['table']),
            condition=sql.SQL(kwargs['cond'])
        )
        logging.info(query.as_string(conn))
        cur.execute(query)
        result= cur.fetchall()
        
    if result is None:
        logging.warning(f">> No Data!!")
        logging.warning(f">> query = {query.as_string(conn)}")
        
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
        logging.info(query.as_string(conn))
        for row in args[0]:
            logging.debug(row)
            cur.execute(query, tuple(row))

@ConnSQL
def __truncate_table(*args, conn, **kwargs):
    with conn.cursor() as cur:
        query = sql.SQL("TRUNCATE TABLE {table};").format(
            table=sql.SQL(kwargs['table'])
        )
        cur.execute(query)
        
def fetch_road_district():
    logging.info('|----- Fetch road_names Data -----|')
    data = __fetch_data(
        cols=['*'],
        table='public.road_names'
    )
    return data

def insert_statics_data(data):
    logging.info('|----- Insert On-street Statics Data -----|')
    static_cols = ['grid_id', 'cell_id', 'name', 'day', 'hour', 'pay', 'paycash',
                'road_id', 'road_name', 'district', 'lat', 'lon', 'infodate']  

    __insert_data(
        data,
        table='public.on_street_static',
        cols=static_cols,
        pkey=['grid_id']
    )
    
def insert_road_names_data(data):
    logging.info('|----- Insert On-street Road Names Data -----|')
    
    __insert_data(
        data,
        table='public.road_names',
        cols=['road_code', 'road_name'],
        pkey=['road_code']
    )

def truncate_table(table_name):
    option = ['public.on_street_static', 'public.road_names']
    if table_name not in option:
        logging.error(f'>> {table_name} cannot use truncate!!')
        return
    logging.info(f'|----- Truncate {table_name} -----|')
    __truncate_table(table=table_name)