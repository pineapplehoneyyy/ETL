import sqlalchemy
import psycopg2
import os
import pandas as pd 

class DatabaseHelper:

    """
    create database connection 
    create new tbl if needed 
    upload dataframe 
    """
    
    def __init__(self,dbname,user,password,host,port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn_string = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'\
        .format(user=self.user, password=self.password, host=self.host, port=self.port, dbname=self.dbname)
        self.db = sqlalchemy.create_engine(self.conn_string)      

    def create_db_tbl(self,sql_new_tbl_string):
        try:
            pg_conn = psycopg2.connect(self.conn_string)
            cursor = pg_conn.cursor()
            cursor.execute(sql_new_tbl_string)
            pg_conn.commit()
            cursor.close()
            pg_conn.close()
        except Exception as e:
            print('Failed to create db table')

    def upload_df_to_db(self,df,tbl_name):
        try:
            conn = self.db.connect()
            df.to_sql(tbl_name, conn, if_exists='replace', index=False)
            conn.close()
        except Exception as e:
            print('Failed to upload to {} due to {}'.format(tbl_name,e))   
