import psycopg2
from sqlalchemy import create_engine

class PostgreSQL:
    def sql_conn(self):        
        # engine = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}".format(
        #     user='root',
        #     pw='thi168168',
        #     host='220.130.185.36',
        #     port='5432',
        #     db='postgres'
        # ))
        engine = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}".format(
            user='thi',
            pw='thi',
            host='172.18.16.198',
            port='5432',
            db='postgres'
        ))
        return engine