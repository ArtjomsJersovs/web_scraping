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
from selenium import webdriver
from selenium.webdriver.common.by import By
#set chromodriver.exe path
from fp.fp import FreeProxy

proxy = FreeProxy(rand=True, timeout=1,anonym=True).get()

def set_viewport_size(driver, width, height):
    window_size = driver.execute_script("""
        return [window.outerWidth - window.innerWidth + arguments[0],
          window.outerHeight - window.innerHeight + arguments[1]];
        """, width, height)
    driver.set_window_size(*window_size)

options = webdriver.ChromeOptions()


options.add_argument("--disable-blink-features")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("start-maximized")
user_agent = "ttt" 
options.add_argument("user-agent=%s" % user_agent) 
#options.add_argument('--proxy-server=%s' % proxy)
driver = webdriver.Chrome(options=options, executable_path='C:/Users/artjoms.jersovs/Documents/chromedriver.exe')
set_viewport_size(driver, 800, 600)

cookies = {
    'name':'name',
    'value':'value',
    'PHPSESSID':'dafa9416c9fa15089fcb49a537e21c0d',
    'sid':'53a89091df20e5c228b9f4a895e09efe2ca19e52fe1cd2f3b14c778442d9fcc8263bf3e461e620d9ba4964c5e1982c7c',
    '__gads=ID=f539e122f23ea838-22d6c8d510ce00f4:T=1662107175:RT=1662107175:S':'ALNI_MadSzUuqF5tt0iCitPwEFaOMydGhw',
    '__gsas=ID=02fd7f01b0d014eb:T=1664614153:S':'ALNI_MYyViT0C58VcKSD-Nj5uBUk-ahQDA',
    'LG':'lv',
    '_ga':'GA1.2.1502828473.1665090460',
    'sid_c':'1'}

driver.add_cookie(cookies)
driver.implicitly_wait(2)
#launch URL
driver.get('https://www.ss.lv')
driver.get('https://www.ss.lv/transport/cars')
driver.get('https://www.ss.lv/transport/cars/alfa/')
driver.get('https://www.ss.lv/msg/lv/transport/cars/alfa-romeo/159/blgxih.html')

driver.find_elements(By.CLASS_NAME, 'td7')[1].click()

driver.find_element(By.XPATH, '//*[@id="tdo_1678"]/a').click()

driver.quit()

html = requests.get('https://www.ss.lv/msg/lv/transport/cars/alfa-romeo/giulia/cbiild.html')

