import win32com.client

# Запуск Excel с выбором клиента
excel = win32com.client.Dispatch("Excel.Application")

# Открытие рабочей книги
workbook = excel.Workbooks.Open(r'path/to/dir/file.xlsx')

# Получение PowerPivot объекта
powerPivot = workbook.Model

# Обновление модели данных
powerPivot.Refresh()

# Сохранение и закрытие рабочей книги
workbook.Save()
workbook.Close()

excel.Quit()

