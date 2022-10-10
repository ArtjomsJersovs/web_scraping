import requests
import pandas as pd
import json
import subprocess

### FUNCTIONS
def clearvars():    
    for el in sorted(globals()):
        if '__' not in el:
                print(f'deleted: {el}')
                del el

def save_file(file_name, source):
    with open(file_name, 'w', encoding='utf8') as file:
        if str.split(file_name,'.')[-1]=='html':
            src = source.text
            file.write(src)
        elif str.split(file_name,'.')[-1]=='csv':
            source.to_csv(file_name, index = False, encoding='utf8')
        elif str.split(file_name,'.')[-1]=='xlsx':
            source.to_excel(file_name, sheet_name = 'Data', index = False)
        elif str.split(file_name,'.')[-1]=='json':
            json.dump(source, file, indent=4, ensure_ascii=False)
        else:
            file.writelines(source)
        

def excel_export(df, name='temp_file', size=10000):
    df.head(int(size)).to_excel(str(name) +".xlsx") 
    subprocess.run(["C:/Program Files/Microsoft Office/root/Office16/EXCEL.exe", str(name) +".xlsx"])
