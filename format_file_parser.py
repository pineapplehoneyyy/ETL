import pandas as pd 
import os

class FormatFileParser:

    """ 
    a class parser wrapper to generate corresponding 
    format details from specs file, and parse data into 
    data frames with proper data types
    """
    
    def __init__(self,format_name):
        self.col_names = None
        self.col_width = None
        self.col_datatype = None
        format_files = os.listdir("specs/") 
        for f in format_files:
                if f == format_name +'.csv':
                    self.data = pd.read_csv("specs/"+f)
                    self.format_column_names()
                    self.format_column_width()
                    self.format_column_data_type()
                    break
            
    def format_column_names(self):
        self.col_names = list(self.data['column name'])
        return self.col_names

    def format_column_width(self):
        self.col_width = list(self.data['width'])
        return self.col_width

    def format_column_data_type(self):
        self.col_datatype = list(self.data['datatype'])
        return self.col_datatype 

    # this function uses data width as index to slice data
    def parse_data(self,f):
        if self.col_names is None or self.col_width is None:
            print("Column names and column width must be initialized...")
            return None
        new_data_dic = {}
        for i in self.col_names:
            new_data_dic[i] = []
        with open("data/"+f,'r') as d:    
            lines = d.read().splitlines()
            for line in lines:
                j = 0
                for i, v in enumerate(self.col_names):
                    new_data_dic[v].append(line[j:(self.col_width[i]+j)].strip())
                    j+=self.col_width[i]
        return new_data_dic
    
    # this function converts data types to df data types
    def convert_data_type(self,df):
        if self.col_names is None or self.col_datatype is None:
            print("Column names and column data type must be initialized...")
            return None
        # create a dictionary for dataframe data types
        df_data_type_dic = {
            'TEXT':'str', 
            'BOOLEAN':'bool',
            'INTEGER':'int'
        }
        for i, v in enumerate(self.col_names):
            # to convert 1&0 to int first then change type to bool
            if self.col_datatype[i] == 'BOOLEAN':
                df[v]=df[v].astype('int')
            df[v]=df[v].astype(df_data_type_dic[self.col_datatype[i]])
        return df

    def add_df_col(self,df,new_col_name,new_col_datatype,new_col_data):
        df[new_col_name] = new_col_data
        df[new_col_name] = df[new_col_name].astype(new_col_datatype)
        return df

    # this function creates sql statement to be used to create new table if not exists
    def create_sql_tbl_string(self, df, tbl_name):
        sql_data_type_dic = {
            'object':'varchar', 
            'bool':'bool',
            'int64':'int',
            'datetime64[ns]':'date'
        }
        col_string = ', '.join('{} {}'.format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(sql_data_type_dic)))
        sql_new_tbl_string = 'CREATE TABLE IF NOT EXISTS {} ({});'.format(tbl_name,col_string)
        return sql_new_tbl_string
