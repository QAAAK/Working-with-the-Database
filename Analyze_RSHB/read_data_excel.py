import get_connection
import pandas as pd
import openpyxl



def read_excel (query:str) :
    
    connection = get_connection()
    
    try:
        df = pd.read_sql_query(query, connection, index_col=None)
        
        # create excel-file
        filepath = "./Desktop/{0}.xlsx".format(query)
        wb = openpyxl.Workbook()
        wb.save(filepath)
        
        # read excel-file sql-query
        
        df.to_excel(filepath, index=0)
        
        return 'Файл загружен на рабочий стол {0}.xlsx'.format(query)
    
    except:
        
        return 'Ошибка'
    
    
    
