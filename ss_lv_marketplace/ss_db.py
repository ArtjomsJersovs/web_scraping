import sqlite3
from sqlite3 import Error
import pandas as pd

database = r"C:\Users\artjoms.jersovs\github\Scraping\web_scraping\ss_lv_marketplace\ss_db.db"

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = sqlite3.connect(db_file)
    print(sqlite3.version)
    return conn

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

sql_create_offers_table = """ CREATE TABLE IF NOT EXISTS ss_lv_car_offers (
                                    id integer PRIMARY KEY,
                                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    auto_label text,
                                    link text,
                                    description text,
                                    model text,
                                    year integer,
                                    engine_cap text,
                                    mileage integer,
                                    price float,
                                    transmission text,
                                    color text,
                                    body_type text,
                                    inspection_valid text,
                                    publish_date datetime,
                                    uniq_offer_id integer
                                ); """



conn = create_connection(r'C:\Users\Administrator\Documents\web_scraping\ss_lv_marketplace\ss_lv.db')

create_table(conn, sql_create_offers_table)

def insert_many_records(record_list, db_file):
    try:
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
            
main_df = main_df.drop('created_at', axis=1)

recordsToInsert = list(main_df.itertuples(index=False))

insert_many_records(recordsToInsert, database)



if __name__ == '__main__':
    conn = create_connection(database)
    





# def select_all(conn):
#     """
#     Query all rows in the tasks table
#     :param conn: the Connection object
#     :return:
#     """
#     cur = conn.cursor()
#     cur.execute("SELECT * FROM projects")
#     rows = cur.fetchall()
#     df = pd.DataFrame(rows, columns = ['index','col1','col2','col3'])
#     return(df)

# data = select_all(conn)

