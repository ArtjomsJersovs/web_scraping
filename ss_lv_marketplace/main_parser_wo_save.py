from asyncio.windows_events import NULL
from cmath import nan
from pickletools import int4
import re
from bs4 import BeautifulSoup
from matplotlib.pyplot import text
from numpy import NaN
import requests
import bs4 
import os 
from time import sleep
import json
import stored_functions as sf
import random
import csv
import pandas as pd
import sqlite3
from sqlite3 import Error

database = r"C:\Users\artjoms.jersovs\github\Scraping\web_scraping\ss_lv_marketplace\ss_db.db"

headers = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
}

html = requests.get('https://www.ss.lv/lv/transport/cars/')
#sf.save_file('ss.html', html)

# with open("ss.html", encoding="utf8") as file:
#     src = file.read()
soup = BeautifulSoup(html.text, "lxml")
all_cats = soup.find_all("a", class_='a_category')

#Get all category links
all_cats_dict = {}
for cat in all_cats:
    cat_link = 'https://www.ss.lv' + cat.get('href')
    cat_name = cat.get_text()
    all_cats_dict[cat_name] = cat_link   

#subset particular category (temporary)
subset_cat_dict = {'Alfa Romeo': 'https://www.ss.lv/lv/transport/cars/alfa-romeo/'}
subset_cat_dict = dict()
keywords = ['Smart','Alfa Romeo']

for (key, value) in all_cats_dict.items():
   # Check if key is even then add pair to new dictionary
   if any(keyword in key for keyword in keywords): 
       subset_cat_dict[key] = value
   else:
       print('No subselected brands provided!')

# Iterate over all the items in dictionary and filter items which has even keys
#iterate over each category
iteration_count = int(len(subset_cat_dict)) - 1
count = 0
print(f"Iterations in total across categories: {iteration_count}")

#create dataframe to store all parsed content
main_df = pd.DataFrame()

#Iterate through all categories (or particular category)
for name, links in subset_cat_dict.items():  #all_cats_dict.items()
#limit only first x categories (testing purposes)
    if count <= 1:
        
#iterate over all pages and parse content
        seq_nr = 1 #unique urls starts only from page nr.2
        while True:
            name_raw = links.split('/')[-2]
            #different html structure for first and other pages
            if seq_nr==1:
                src_subcat = requests.get(f'https://www.ss.lv/lv/transport/cars/{name_raw}/', headers=headers)
            else:
                src_subcat = requests.get(f'https://www.ss.lv/lv/transport/cars/{name_raw}/page{seq_nr}.html', headers=headers)
                
                

            cat_soup = BeautifulSoup(src_subcat.text, "lxml")
            sleep(random.randrange(2, 3))
            print(f'{name} - page Nr. {seq_nr} is saved..')
            #counter for requesting pages
            seq_nr += 1

            # auto_label = 'Marka'
            # link = 'Link'
            # #get headers names for data stored on page
            # description = cat_soup.find('tr',id='head_line').find('td').find('span').text.strip()
            # others = cat_soup.find('tr', id='head_line').find_all(class_='msg_column_td')

            df = pd.DataFrame(columns =['auto_label', 'link', 'description', 'model', 'year', 'engine_cap', 'mileage', 'price', 'transmission', 'color', 'body_type', 'inspection_valid', 'publish_date', 'uniq_offer_id'])
            
            sludinajumi_all = cat_soup.find('tr', id='head_line').find_next_siblings()[:-1]
            sludinajumi_content = []
            
            #iterate over each offer on each page to store general data
            for item in sludinajumi_all:
                all_content = item.find_all('td')
                auto_label = name
                created_at = NaN
                link = 'http://ss.lv'+item.find('a').get('href')
                model = all_content[3].text
                try:
                    year = int(re.sub('[^0-9]' ,'',all_content[4].text))
                except:
                    year = NaN
                engine_cap = all_content[5].text
                try:
                    mileage = float(re.sub('[^0-9]' ,'',all_content[6].text))
                except:
                    mileage = NaN
                try:
                    price = float(re.sub('[^0-9]' ,'',all_content[7].text))
                except:
                    price = NaN
                try:
                    uniq_offer_id = int(re.sub('[^0-9]','',item.find(class_='msg2').find('a').get('id')))
                except:
                    uniq_offer_id = NaN
                #get inside particular offer and parse data
                html_offer = requests.get(link)
                soup_offer = BeautifulSoup(html_offer.text, "lxml")
                #handle buy offers as they don't have filled content
                try: 
                    description = soup_offer.find('div', id='msg_div_msg').find_all(text=True, recursive=False)[1].strip()
                except:
                    description = ''
                #get detailed data from particular offer and handle error if one of details doesn't exist
                try:
                    transmission = soup_offer.find(class_='options_list').find('td', string="Ātr.kārba:").next_sibling.text
                except:
                    transmission = ''
                try:
                    color = soup_offer.find(class_='options_list').find('td', string="Krāsa:").next_sibling.text
                except:
                    color = ''
                try:
                    body_type = soup_offer.find(class_='options_list').find('td', string="Virsbūves tips:").next_sibling.text
                except:
                    body_type = ''
                try:
                    inspection_valid = soup_offer.find(class_='options_list').find('td', string="Tehniskā apskate:").next_sibling.text
                except:
                    inspection_valid = ''
                try:
                    publish_date = re.sub('[a-zA-Z]' ,'',soup_offer.find_all('tr')[-3].find_all('td')[-1].text)[2:]
                except:
                    publish_date = ''                   
                    
                #create record with data in same structure aas main dataframe
                df = df.append(
                    {
                        "auto_label":auto_label,
                        "link": link,
                        "description": description,
                        "model": model,
                        "year": year,
                        "engine_cap": engine_cap,
                        "mileage": mileage,
                        "price": price,
                        "transmission": transmission,
                        "color": color,
                        "body_type": body_type,
                        "inspection_valid": inspection_valid,
                        "publish_date" : publish_date,
                        "uniq_offer_id": uniq_offer_id
                    },
                    ignore_index=True
                )
                sleep(random.randrange(1, 2))
                
            #append particular offer content to empty dataframe with columns names
            main_df.reset_index(drop=True, inplace=True)
            df.reset_index(drop=True, inplace=True)
            main_df = pd.concat([main_df, df])


            #stop when all existing pages are collected (next page doesnt exist)
            try:
                if re.sub("[^0-9]", "",cat_soup.find_all(class_='navi')[-1].get('href'))=='':
                    break   
            except:
                break

        #Iterator for convenience once particular acetogry is fully scraped    
        print(f'all pages for {name} are saved!')
        #counter to take next category from primary list of all categories
        count += 1
        print(f"# Iteration {count}. {name} is saved..")
        #how many categories left to be scraped
        iteration_count = iteration_count - 1
        #stop scraper once all categories are scraped
        if iteration_count == 0:
            print("Job is finished")
            break
        
        #save mani dataframe in project folder
        sf.save_file('all_cats_content.xlsx', main_df)
        
        print(f"Category iterations remained: {iteration_count}")
        sleep(random.randrange(2, 3))
        

