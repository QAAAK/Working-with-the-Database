import pandas as pd
import os

def read_excel_to_csv():    
    arr = []
    
    for file in os.listdir(r'C:\Users\Santalov-dv\Desktop\Excel'):
        arr.append(file)

    for i in arr:        
        try:
            read_file = pd.read_excel (r'C:\Users\Santalov-dv\Desktop\Excel\{i}'.format(i)) 
            read_file.to_csv (r"C:\Users\Santalov-dv\Desktop\csv\test.csv",  
                                index = None, 
                                header=True) 
        except:
            print( "Неверное имя файла")
   
    return 'Passed!'
            
read_excel_to_csv()
