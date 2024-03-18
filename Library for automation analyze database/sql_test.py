import sqlite3
import pandas as pd

# from size_memory import convert_size


connection = sqlite3.connect(r'C:\Users\Santalov-dv\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db')


# df = pd.read_excel(r"C:\Users\Santalov-dv\Desktop\parser_excel\Новая таблица.xlsx")
# name_table = df['Имя сущности'].tolist()


cursor = connection.cursor()



def typeof_and_count_attr(name_table, df):
    
    """Определяет тип данных, имя отрибута и количество атрибутов

    Args:
        name_table ([]): имена таблиц в массиве; 
        df (_type_): DataFrame;

    Returns:
        Возвращает уведомление о завершении
    """
    
    df['Количество атрибутов'] = ''
    df['Имя атрибута и тип данных'] = ''
    name = ''

    for i in range(0,len(name_table)):
        
        sql_table = pd.read_sql(sql = f'SELECT * FROM {name_table[i]}', con = connection)
        sql_name_columns = sql_table.columns.tolist()
        
        for j in range(0, len(sql_name_columns)):
            
            row = cursor.execute(f'SELECT typeof({sql_name_columns[j]}) FROM {name_table[i]}')
            type_column = row.fetchone()
            name += f'{sql_name_columns[j]} -- {type_column}\n'
            
        df.at[i, 'Имя атрибута и тип данных'] = name
        df.at[i, 'Количество атрибутов'] = len(sql_name_columns)
        
        print(f'typeof_and_count_attr -- Проанализирована таблица {name_table[i]}')
        
        name = ''
        
    return 'Процесс завершен (typeof_and_count_attr)'


def count_row(name_table, df):
    
    """Определяет количество строк в таблице (Если таблица пустая, помечает ее)

    Args:
        name_table ([]): имена таблиц в массиве; 
        df (_type_): DataFrame;

    Returns:
        Возвращает уведомление о завершении
    """
    
    df['Количество строк'] = ''
    
    for i in range(0, len(name_table)):
        ex = cursor.execute(f'SELECT COUNT(*) FROM {name_table[i]}')
        row =ex.fetchone()
        
        if row[0] != 0:
            df.at[i, 'Количество строк'] = row[0]
        else:
            df.at[i, 'Количество строк'] = 'ПУСТАЯ ТАБЛИЦА'
        
        print(f'count_row -- Проанализирована таблица {name_table[i]}')
    
    return 'Процесс завершен (count_row)'
        
#     del ex, row
        
        
# for i in range(0, len(name_table)):

#     df['Дубликаты'] = ''
    
#     sql_table = pd.read_sql(sql = f'SELECT * FROM {name_table[i]}', con = connection)
#     sql_name_columns = sql_table.columns.tolist()
#     row = cursor.execute(f'SELECT COUNT({sql_name_columns[0]}) FROM {name_table[i]} GROUP BY {sql_name_columns[0]} HAVING COUNT({sql_name_columns[0]}) > 2')
#     count = row.fetchall()
    
#     if len(count) == 0:
#         df.at[i, 'Дубликаты по id'] = '-'
#     else:
#         df.at[i, 'Дубликаты по id'] = f'{sql_name_columns[0]} -- {count}'
        
#     del sql_table, sql_name_columns, row, count
        
def min_date_and_max_date(name_table, df):
    
    """Определяет первую дату загрузки данных и последнюю дату

    Args:
        name_table ([]): имена таблиц в массиве; 
        df (_type_): DataFrame;

    Returns:
        Возвращает уведомление о завершении
    """

    df['Минимальная и максимальная дата'] = ''
    date = [('date',), ('datetime',), ('datetime2',), ('smalldatetime',), ('time',), ('datetimeoffset',)]
    date_ = ''

    for i in range(0,len(name_table)):
        sql_table = pd.read_sql(sql = f'SELECT * FROM {name_table[i]}', con = connection)
        sql_name_columns = sql_table.columns.tolist()
        
        for j in range(0, len(sql_name_columns)):
            row = cursor.execute(f'SELECT typeof({sql_name_columns[j]}) FROM {name_table[i]}')
            type_column = row.fetchone()
            
            if type_column in date:
                row_date = cursor.execute(f'SELECT {sql_name_columns[j]} FROM {name_table[i]} ORDER BY {sql_name_columns[j]} DESC LIMIT 1')
                min_date = row_date.fetchone()
                date_ = f'Минимальная дата атрибута {sql_name_columns[j]} -- {min_date[0]}\n'
                
                
                row_date = cursor.execute(f'SELECT {sql_name_columns[j]} FROM {name_table[i]} ORDER BY {sql_name_columns[j]} ASC LIMIT 1')
                max_date = row_date.fetchone()
                date_ += f'Максимальная дата атрибута {sql_name_columns[j]} -- {max_date[0]}'
                df.at[i, 'Минимальная и максимальная дата'] = date_
                
                
            else:             
                df.at[i, 'Минимальная и максимальная дата'] = 'Нет атрибута с типом данных DATE'
                       
        print(f'min_date_and_max_date -- Проанализирована таблица {name_table[i]}')
            
    return 'Процесс завершен (min_date_and_max_date)'


def duplicate_column(name_table, df):
    
    df['Список таблиц с повторяющимся элементами'] = ''
    arr_count = []       
    sql_arr = []
    for i in range(0, len(name_table)):
        
        sql_table = pd.read_sql(sql = f'SELECT * FROM {name_table[i]}', con = connection)
        sql_name_columns = sql_table.columns.tolist()
        sql_arr.append(sorted(sql_name_columns))
        
    for j in range(0, len(sql_table)):
        for j in range(0, len(sql_arr)):
            duplicate = sql_arr.count(sql_arr[j])
            if duplicate > 1:
                arr_count.append(name_table[i])

        df.at[i, 'Список таблиц с повторяющимся элементами'] = arr_count
        
        print(f'Проанализирована таблица {name_table[i]}')

            
    return 'Процесс завершен (duplicate_column)'        
        
# process = psutil.Process()
# process = process.memory_info().rss
        

# df.to_excel(r'C:\Users\Santalov-dv\Desktop\parser_excel\Test_table_3.xlsx', index=False)


# print(f'Занимаемый объем выполнения программы {convert_size(process)}')

def primary_key(name_table, df):
    
    """Поиск первичного ключа таблицы

    Args:
        name_table ([]): имена таблиц в массиве
        df (_type_): DataFrame

    Returns:
        Возвращает уведомление о завершении
    """
    
    df['Первичный ключ'] = ''
    
    for i in range(0, len(name_table)):
        row = cursor.execute(f'SELECT column_name FROM all_cons_columns WHERE constraint_name = (SELECT constraint_name FROM user_constraints WHERE UPPER(table_name) = UPPER({name_table[i]}) AND CONSTRAINT_TYPE = "P")')
        pk = row.fetchall()
        
        if pk != []:
            df.at[i, 'Первичный ключ'] = pk
        else:
            df.at[i, 'Первичный ключ'] = 'Первичный ключ не найден'
            
        print(f'primary_key -- Проанализирована таблица {name_table[i]}')
             
    return 'Процесс завершен (primary_key)'

def last_analyze(name_table, df):
    pass


def get_create_table (name_table, df):
    
    """Получает скрипт таблицы

    Args:
        name_table ([]): массив имен сущности
        df (_type_): DataFrame

    Returns:
        Возвращает результат о выполнении
        
    """
    df['скрипт написания таблицы (ddl)'] = ''
    
    for i in range(0, len(name_table)):
        row = cursor.execute(f'SELECT DBMS_METADATA.GET_DDL("TABLE", "{name_table[i]}")')
        ddl = row.fetchall()
        df.at[i, 'скрипт написания таблицы (ddl)'] = ddl
        print(f'get_create_table -- проанализирована таблица {name_table[i]}')
        
    return 'процесс завершен (get_create_table)' 


def column_name_and_type_length_comments(path_excel, sheet):
    
    """Вовзращает имена атрибутов, тип данных, длинц и комментарии (имя атрибута) 
    
    Args:
        path (str): путь к файлу Excel
        df (_type_): имя листа на который будет происходить запись


    Returns:
       Возвращает результат о выполнении
    """
       
    path = path_excel
    query = """SELECT all_tab.table_name as 'Имя таблицы', 
                all_tab.column_name as 'Имя атрибута',
                all_tab.data_type as 'Тип данных',
                all_tab.data_length as 'Длина данных',
              (SELECT COMMENTS
                FROM user_col_comments t
                where t.TABLE_NAME = all_tab.TABLE_NAME
                and t.COLUMN_NAME = all_tab.column_name) as 'Комментарии (имя атрибута)'
              FROM all_tab_columns all_tab"""
              
    df = pd.read_sql(query, connection)
    
    df.to_excel(path, sheet_name=sheet)
    
    return 'Процесс завершен (column_name_and_type_length_comments)'

def histories(path_excel, sheet):
    
    """Проверяет историчность данных
    
    Args:
        path (str): путь к файлу Excel
        df (_type_): имя листа на который будет происходить запись


    Returns:
       Возвращает результат о выполнении
    """
    

    
    df = pd.read_excel(path_excel, sheet_name=sheet)
    query = 'SELECT m.table_name as "Имя таблицы", o.created as "Дата создания", m.timestamp as "Дата последнего обновления" ROUND(m.timestamp - o.created) as "DIFF_DATE" FROM all_objects o INNER JOIN all_tab_modifications m ON o.object_name = m.table_name'
    df_sql = pd.read_sql(query, con = connection)

    df = df.merge(df_sql,how = 'inner')
    
    df.to_excel(path_excel, sheet_name = sheet, index = False)
    
    return 'Процесс завершен (histories)'