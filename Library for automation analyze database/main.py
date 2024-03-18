import sqlite3
import pandas as pd
import psutil

import sql_test
from size_memory import convert_size


connection = sqlite3.connect(r'C:\Users\Santalov-dv\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db')

cursor = connection.cursor()


path = r"C:\Users\Santalov-dv\Desktop\parser_excel\Новая таблица.xlsx"


df = pd.read_excel(path)
name_table = df['Имя сущности'].tolist()



# print(sql_test.typeof_and_count_attr(name_table, df))
# print(sql_test.count_row(name_table, df))
# #print(sql_test.min_date_and_max_date(name_table, df))
# #print(sql_test.duplicate_column(name_table, df))
# #print(sql_test.primary_key(name_table, df))

cursor.close()


df.to_excel(r'C:\Users\Santalov-dv\Desktop\parser_excel\Test_table_4.xlsx', index=False)


process = psutil.Process()
process = process.memory_info().rss


print(f'Занимаемый объем выполнения программы {convert_size(process)}')


