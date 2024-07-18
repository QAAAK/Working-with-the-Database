import pandas as pd
import os

arr = []
    
for file in os.listdir(r'C:\Users\Santalov-dv\Desktop\Excel'):
    arr.append(file)
    
print(arr)
