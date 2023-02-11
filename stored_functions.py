import requests
import pandas as pd
import json
import subprocess
import sqlite3
from sqlite3 import Error

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


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = sqlite3.connect(db_file)
    print(sqlite3.version)
    return conn

def insert_many_records_ss_lv_car_offers(df, db_file):
    try:
        record_list = list(df.itertuples(index=False))
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print("Connected to SQLite")

        sqlite_insert_query = """INSERT INTO ss_lv_car_offers
                          (auto_label, link, description, model, year, engine_cap, mileage, price, transmission, color, body_type, inspection_valid, publish_date, uniq_offer_id) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""

        cursor.executemany(sqlite_insert_query, record_list)
        conn.commit()
        print("Total", cursor.rowcount, "Records inserted successfully into ss_lv_car_offers table")
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to insert multiple records into sqlite table", error)
    finally:
        if conn:
            conn.close()
            print("The SQLite connection is closed")
            
def select_index_from_ss_lv_car_offers(db_file):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT uniq_offer_id FROM ss_lv_car_offers")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns = ['uniq_offer_id'])
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to fetch sqlite table", error)
    finally:
        if conn:
            conn.close()
            print("The SQLite connection is closed")
    return(df)

