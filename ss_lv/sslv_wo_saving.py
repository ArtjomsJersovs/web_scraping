import re
from bs4 import BeautifulSoup
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
    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1"
}

#MAIN HTML
sf.save_file('ss.html', html)
with open("ss.html", encoding="utf8") as file:
    src = file.read()
soup = BeautifulSoup(src, "lxml")
all_cats = soup.find_all("a", class_='a_category')

#ALL CATEGORIes LINKS
all_cats_dict = {}
for cat in all_cats:
    cat_link = 'https://www.ss.lv' + cat.get('href')
    cat_name = cat.get_text()
    all_cats_dict[cat_name] = cat_link   

#subset_auto_company = {'Volkswagen':'https://www.ss.lv/lv/transport/cars/volkswagen/'}
#iterate over each category
iteration_count = int(len(all_cats_dict)) - 1
count = 0
print(f"Iterations in total across categories: {iteration_count}")

#create dataframe to store all parsed content
main_df = pd.DataFrame()

for name, link in all_cats_dict.items():
#limit only first 4 categories (testing purposes)
    if count <= 1:
        
#iterate over all pages and parse content
        seq_nr = 1 #unique urls starts only from page nr.2
        while True:
            name_raw = link.split('/')[-2]
            if seq_nr==1:
                src_subcat = requests.get(f'https://www.ss.lv/lv/transport/cars/{name_raw}/', headers=headers)
            else:
                src_subcat = requests.get(f'https://www.ss.lv/lv/transport/cars/{name_raw}/page{seq_nr}.html', headers=headers)
                
            cat_soup = BeautifulSoup(src_subcat.text, "lxml")
            sleep(random.randrange(2, 3))
            print(f'{name} - page Nr. {seq_nr} is saved..')
            seq_nr += 1

            auto_label = 'Marka'
            links = 'Link'
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
            
            for item in sludinajumi_all:
                all_content = item.find_all('td')
                auto_label = name
                links = 'ss.lv'+item.find('a').get('href')
                sludinajums = all_content[2].text
                modelis = all_content[3].text
                gads = all_content[4].text
                tilpums = all_content[5].text
                nobraukums = all_content[6].text
                cena = all_content[7].text
                sludinajuma_id = item.find(class_='msg2').find('a').get('id')

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
                        "uniq_id": sludinajuma_id
                    },
                    ignore_index=True
                )
#add collected content for each page html
            main_df.reset_index(drop=True, inplace=True)
            df.reset_index(drop=True, inplace=True)
            main_df = pd.concat([main_df, df])


#stop when all existing pages are collected
            if re.sub("[^0-9]", "",cat_soup.find_all(class_='navi')[-1].get('href'))=='':
                break   

#Iterator for convenience          
        print(f'all pages for {name} are saved!')
        
        count += 1
        print(f"# Iteration {count}. {name} is saved..")
        iteration_count = iteration_count - 1

        if iteration_count == 0:
            print("Job is finished")
            break
        
        #save mani dataframe in project folder
        sf.save_file('all_cats_content.xlsx', main_df)
        
        print(f"Category iterations remained: {iteration_count}")
        sleep(random.randrange(2, 3))
        

