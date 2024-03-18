import sqlite3



connection = sqlite3.connect(r'C:\Users\Santalov-dv\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db')
cursor = connection.cursor()


def insert_cascade(count, name_table, column, values): 
    """
    Каскадное добавление строк в таблицу
    Args:
        count (int): Количество строк добавления в таблицу; 
        name_table (str): Имя таблицы;
        column (list): Поля для заполнения;
        values (list): данные для загрузки;
    Returns:
        Возвращает уведомление о выполнении
    """      
    for i in range(0,count):
        cursor.execute(f'INSERT INTO {name_table}{column} VALUES ({values})')
    connection.commit()
    return f'добавлены строки'

def insert(name_table, column, values): 
    """
    Добавление строк в таблицу
    Args: 
        name_table (str): Имя таблицы;
        column (list): Поля для заполнения;
        values (list): данные для загрузки;
    Returns:
        Возвращает уведомление о выполнении
    """
    
    cursor.execute(f'INSERT INTO {name_table}({column}) VALUES ({values})')
    print(f'Данные загружены в таблицу {name_table}')
    
    connection.commit()
    
    return f'добавлены строка'


cursor.close()
        
        
        
        
        
        
        
    