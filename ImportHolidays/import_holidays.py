import csv
import logging
import psycopg2
from datetime import datetime, timedelta
from psycopg2 import sql

class ConnSQL:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        conn = psycopg2.connect(
          database='postgres',
          user='thi',
          password='thi',
          host='localhost',
          port='5432')
        try:
            result = self.func(*args, conn=conn, **kwargs)
        except ValueError as err:
            conn.rollback()
            logging.error(f'Unexpected {err}, {type(err)}')
            raise
        else:
            conn.commit()
            logging.info(f'>> SQL Successfully!!')
        finally:
            conn.close()
        return result

@ConnSQL
def __insert_data(*args, conn, **kwargs):
    with conn.cursor() as cur:
        query = sql.SQL("INSERT INTO {table} ({col}) VALUES ({value});").format(
            table=sql.SQL(kwargs['TableName']),
            col=sql.SQL(',').join([sql.SQL(col_name) for col_name in kwargs['Columns']]),
            value=sql.SQL(',').join([sql.SQL('%s') for _ in range(len(kwargs['Columns']))])
        )
        for row in args[0]:
            cur.execute(query, tuple(row))

def insert_holidays(data):
    logging.info('|----- Insert holidays Data -----|')
    kwargs = {}
    kwargs['TableName'] = f'holidays'
    kwargs['Columns'] = 'date_col week_no is_weekend is_national is_other is_makeup is_workday is_billing'.split(' ')

    __insert_data(
        data,
        TableName=kwargs['TableName'],
        Columns=kwargs['Columns']
    )

def read_csv():
    with open("/home/thi/holidays.csv", "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        res = []
        for row in reader:
            res.append(row)
    return res

def space2none(data):
    for row in data:
        for i, v in enumerate(row):
            if v == '':
                row[i] = None


if __name__ == "__main__":
    holidays = read_csv()
    space2none(holidays)
    insert_holidays(holidays)
