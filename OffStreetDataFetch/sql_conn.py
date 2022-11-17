import psycopg2
import logging

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
    
    def __sql_execute(self, sql, strings):
        self.cur = self.conn.cursor()
        self.cur.execute(sql, strings)
        
    def insert_dynamic_data(self, data):
        self.sql_conn()
        try:
            table = 'public.off_street_dynamic'
            insert_obj = 'id, availablecar, infotime'
            values = '%s,'*len(insert_obj.split(','))
            values = values[:-1]
            unique_key = 'id, infotime'
            sql = f"INSERT INTO {table} ({insert_obj}) VALUES\
                ({values}) ON CONFLICT ({unique_key}) DO NOTHING;"
            for i, row in enumerate(data):
                print(i/len(data), end='\r')
                self.__sql_execute(sql, tuple(row))
            self.conn.commit()
        except ValueError as err:
            logging.error(f'Unexpected {err}, {type(err)} while INSERT Dynamic Data')
        finally:
            self.conn.close() 