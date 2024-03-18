

# connection mssql.py

import pyodbc

SERVER = '<server-address>'
DATABASE = '<database-name>'
USERNAME = '<username>'
PASSWORD = '<password>'

connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};
                              SERVER={SERVER};
                              DATABASE={DATABASE};
                              UID={USERNAME};
                              PWD={PASSWORD}')


cursor = connection.cursor()