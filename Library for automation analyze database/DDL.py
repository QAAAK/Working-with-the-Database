import sqlite3
# import pandas as pd


connection = sqlite3.connect(r'C:\Users\Santalov-dv\AppData\Roaming\DBeaverData\workspace6\.metadata\sample-database-sqlite-1\Chinook.db')
cursor = connection.cursor()


def create_table(name_table, column):
    """Создание таблицы

    Args:
        name_table (str): Имя таблицы;
        column (list): Поля для создания таблицы;

    Returns:
        Возвращает уведомление о выполнении
    """
    
    column_str = ','.join(column)
    
    cursor.execute(f'CREATE TABLE {name_table} ({column_str})')
    return 'Таблица создана'
    
    

def create_table_cascade(count,name_table, column):
    """Каскадное создание таблицы. Создает однотипные таблицы 
       с одинаковым атрибутным составом

    Args:
        count (int): Количество создаваемых таблиц;
        name_table (str): Имя таблицы;
        column (list): Поля для создания таблицы;

    Returns:
        Возвращает уведомление о выполнении
    """
    
    column_str = ','.join(column)
    
    for i in range(0, count):
        cursor.execute(f'CREATE TABLE {name_table + str(i)} ({column_str})')
    
    return f'Создана {count} таблица '



def drop_table(name_table):
    """Удаляет таблицу

    Args:
        name_table (str): имя таблицы

    Returns:
        ВОзвращает статус о выполнении
    """
    
    cursor.execute(f'DROP TABLE {name_table}')
    
    return f'Таблица {name_table} удалена'


def create_table_like(name_table_new, name_table_old): 
    """Создание таблицы с помощью оператора LIKE
    
    Args:
        name_table_new (list): имя таблицы для создания
        name_table_old (list): таблица с которой формируется новая таблица
    Returns:
        ВОзвращает результат о выполнении
    """
    
    cursor.execute(f'CREATE TABLE {name_table_new} (LIKE {name_table_old} INCLUDING ALL)')
    
    return f'Таблица {name_table_new} создана'
    

def create_table_like_cascade(name_table_new, name_table_old):
    """Каскадное создание таблиц с помощью оператора LIKE

    Args:
        name_table_new (list): имена таблиц для создания
        name_table_old (list): таблицы с которых формируются новые таблицы
        
    Returns:
        Возвращается результат о выполнении
    """

    for i in range(0, len(name_table_new)):
        cursor.execute(f'CREATE TABLE {name_table_new[i]} (LIKE {name_table_old[i]} INCLUDING ALL)')
        print(f'Создана таблица {name_table_new[i]}')
        
        
    return f'Создано {len(name_table_new)} таблиц'
        
        




cursor.close()