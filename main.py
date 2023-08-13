import pyodbc 
import pandas as pd
import os

from format_file_parser import FormatFileParser
from db_helper import DatabaseHelper
from db_info import dbname, user, password, host, port

def main():
    data_files = os.listdir("data/") 
    for f in data_files:

        #get file name and date
        file_name = os.path.basename(f).split('_')
        format_name = file_name[0]
        file_date = file_name[1].split('.')[0]

        # use parser to parse data based on format details 
        # covert df data types 
        # add file date as a new column
        # generate sql string for create new table if not exists
        parser = FormatFileParser(format_name)
        formatted_data = parser.parse_data(f)
        df = pd.DataFrame(formatted_data) 
        converted_df = parser.convert_data_type(df)
        new_df = parser.add_df_col(converted_df,'file_date','datetime64',file_date)
        sql_new_tbl_string = parser.create_sql_tbl_string(new_df, format_name)

        #create database connection, create new tbl if needed and upload df
        db = DatabaseHelper(dbname,user,password,host,port)
        db.create_db_tbl(sql_new_tbl_string)
        db.upload_df_to_db(new_df,format_name)
         
if __name__ == '__main__':
    main()
