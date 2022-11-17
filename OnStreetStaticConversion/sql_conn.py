import os
import psycopg2
from sqlalchemy import create_engine

class PostgreSQL:
        
    def sql_conn(self):        
      engine = create_engine('postgresql://'+\
        os.environ.get('user')+\
        ':'+os.environ.get('password')+\
        '@'+os.environ.get('host')+
        ':'+os.environ.get('port')+
        '/'+os.environ.get('database')+'')
      return engine
        