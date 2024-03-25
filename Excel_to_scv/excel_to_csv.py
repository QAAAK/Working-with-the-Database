import pandas as pd


read_file = pd.read_excel (r"C:\Users\Santalov-dv\Desktop\Excel\Эталон_143_сент_2023.xlsx") 
read_file.to_csv (r"C:\Users\Santalov-dv\Desktop\csv\test.csv",  
                                index = None, 
                                header=True) 



