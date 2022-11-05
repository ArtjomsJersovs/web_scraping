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

html = requests.get('https://www.ss.lv/lv/transport/cars/')

headers = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
}

#MAIN HTML
sf.save_file('ss.html', html)
#extract all categories without duplicate requesting webpage
with open("ss.html", encoding="utf8") as file:
    src = file.read()
soup = BeautifulSoup(src, "lxml")
all_cats = soup.find_all("a", class_='a_category')

#Get all category links
all_cats_dict = {}
for cat in all_cats:
    cat_link = 'https://www.ss.lv' + cat.get('href')
    cat_name = cat.get_text()
    all_cats_dict[cat_name] = cat_link   

#subset particular category (temporary)
subset_auto_company = {'bmw':'https://www.ss.lv/lv/transport/cars/bmw/'}
#iterate over each category
iteration_count = int(len(all_cats_dict)) - 1
count = 0
print(f"Iterations in total across categories: {iteration_count}")

#create dataframe to store all parsed content
main_df = pd.DataFrame()

#Iterate through all categories (or particular category)
for name, link in subset_auto_company.items():  #all_cats_dict.items()
#limit only first x categories (testing purposes)
    if count <= 1:
        
#iterate over all pages and parse content
        seq_nr = 1 #unique urls starts only from page nr.2
        while True:
            name_raw = link.split('/')[-2]
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


            auto_label = 'Marka'
            links = 'Link'
            #get headers names for data stored on page
            sludinajums = cat_soup.find('tr',id='head_line').find('td').find('span').text.strip()
            others = cat_soup.find('tr', id='head_line').find_all(class_='msg_column_td')
            modelis = others[0].text
            gads = others[1].text
            tilpums = others[2].text
            nobraukums = others[3].text
            cena = others[4].text
            sludinajuma_id = 'uniq_id'
            df = pd.DataFrame(columns =[auto_label, links,sludinajums,modelis,gads,tilpums,nobraukums,cena,sludinajuma_id])
            sludinajumi_all = cat_soup.find('tr', id='head_line').find_next_siblings()[:-1]
            sludinajumi_content = []
            
            #iterate over each offer on each page to store general data
            for item in sludinajumi_all:
                all_content = item.find_all('td')
                auto_label = name
                links = 'http://ss.lv'+item.find('a').get('href')
                modelis = all_content[3].text
                gads = all_content[4].text
                tilpums = all_content[5].text
                nobraukums = all_content[6].text
                cena = all_content[7].text
                sludinajuma_id = item.find(class_='msg2').find('a').get('id')
                #get inside particular offer and parse data
                html_offer = requests.get(links)
                soup_offer = BeautifulSoup(html_offer.text, "lxml")
                #handle buy offers as they don't have filled content
                try: 
                    sludinajums = soup_offer.find('div', id='msg_div_msg').find_all(text=True, recursive=False)[1].strip()
                except:
                    sludinajums = ''
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
                    type = soup_offer.find(class_='options_list').find('td', string="Virsbūves tips:").next_sibling.text
                except:
                    type = ''
                try:
                    tech_check = soup_offer.find(class_='options_list').find('td', string="Tehniskā apskate:").next_sibling.text
                except:
                    tech_check = ''
                #create record with data in same structure aas main dataframe
                df = df.append(
                    {
                        "Marka":auto_label,
                        "Link": links,
                        "Sludinājumi": sludinajums,
                        "Modelis": modelis,
                        "Gads": gads,
                        "Tilp.": tilpums,
                        "Nobrauk.": nobraukums,
                        "Cena": cena,
                        "Transmisija": transmission,
                        "Krasa": color,
                        "Virsbuve": type,
                        "tehniska apskate": tech_check,
                        "uniq_id": sludinajuma_id
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
        



