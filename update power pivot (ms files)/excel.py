import os
import glob
import time
import sys
import psycopg2
import win32com.client

from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

load_dotenv()

TABLES = {
    1: ['table_name', 'column']
}

DB_CONFIG = {
    'dbname': 'core',
    'user': os.getenv('USER'),
    'password': os.getenv('PASSWD'),
    'host': os.getenv('HOST'),
    'port': '5432'
}

def update_power_pivot_excel(path):
    # 'Запуск Excel'
    excel = win32com.client.Dispatch("Excel.Application")

    # 'Открытие рабочей книги'
    workbook = excel.Workbooks.Open(path)

    # 'Получение PowerPivot объекта'
    power_pivot = workbook.Model

    # 'Обновление модели данных'
    power_pivot.Refresh()

    # 'Обновление всех соединений данных'
    for connection in workbook.Connections:
        connection.Refresh()

    # 'Сохранение и закрытие рабочей книги'#
    workbook.Save()
    workbook.Close()

    # 'Закрытие Excel'
    excel.Quit()

def check_table(table_name, column_name):
    last_month = datetime.now() - relativedelta(months=2)
    first_day_of_last_month = datetime(last_month.year, last_month.month, 1)
    date_str = first_day_of_last_month.strftime('%Y-%m-%d')

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT '1' FROM {table_name} WHERE {column_name}='{date_str}' LIMIT 1")
            result = cursor.fetchone()

    return result is not None

def get_excel_files():
    excel_files = glob.glob('*.xlsx')

    if not excel_files:
        print('Нет excel файлов')
        time.sleep(5)
        sys.exit()

    return excel_files

def prompt_user_for_file(excel_files):
    options = "\n".join(f"{i+1}\n. {os.path.abspath(file)}" for i, file in enumerate(excel_files))
    prompt = f"Выберете номер из списка. После ввода нажмите Enter: \n{options}"

    while True:
        response = input(prompt)
        if response.isdigit() and 0 < int(response) <= len(excel_files):
            return int(response)

        print("Введите корректное значение")

def update_file():
    excel_files = get_excel_files()
    selected_file_index = prompt_user_for_file(excel_files)
    table_name, column_name = TABLES[selected_file_index]
    if check_table(table_name, column_name):
        print('Начинаю обновление')
        update_power_pivot_excel(os.path.abspath(excel_files[selected_file_index-1]))
        print('Обновление завершено')
    else:
        print('Таблица в источнике не содержит новые данные')
