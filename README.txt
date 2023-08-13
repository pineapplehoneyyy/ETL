*****About the Program:*****
● This python program reads format definitions from specification files 
and use these definitions to load the data files into local postgres database.
● If there is same format data file with a different date, the latter uploaded
data file will replace the original one. 
● If there is new format data files, new table will be created in local database.


*****To Execute:*****
● save your own postgres database info (dbname, user, password, host, port) 
in a db_info.py file, and save under the same directory as the rest of the 
files for this projects
● run "python main.py" in terminal under the directory where the project is saved


*****To Test:*****
you can make up a data file using existing format files or create new format file with 
corresponding data files

Format files will be csv formated with columns "column name", "width", and "datatype".
● "column name" will be the name of that column in the database table
● "width" is the number of characters taken up by the column in the data file
● "datatype" is the SQL data type that should be used to store the value in the database
table.

Data files will be flat text files with lines matching single records for the database. 
Lines are formatted as specified by their associated specification file.


*****Potential Extensions:*****
● scheduling using time or crontab
● email notification for any error and notify once upload job is complete daily
● if incremental upload, can build select_date_from_sql function in DatabaseHelper class 
find max date from database tables, and only upload the ones not uploaded yet
● potential functions in FormatFileParser class to clean data like removing null and remove duplicates
● when share codes, do not include db_info.py or use pickle file to save sensitive info on local machine