from stored_functions import save_file


subset_auto_company = {'Alfa Romeo':'https://www.ss.lv/lv/transport/cars/alfa-romeo/'}
main_df = pd.DataFrame()
name = next(iter(subset_auto_company))
link = next(iter(subset_auto_company.values()))
name_raw = link.split('/')[-2]
src_subcat = requests.get(f'https://www.ss.lv/lv/transport/cars/{name_raw}/', headers=headers)
cat_soup = BeautifulSoup(src_subcat.text, "lxml")
sludinajumi_all = cat_soup.find('tr', id='head_line').find_next_siblings()[:-1]
links = 'http://ss.lv'+sludinajumi_all[0].find('a').get('href')
html_offer = requests.get(links)
soup_offer = BeautifulSoup(html_offer.text, "lxml")
import re

sf.save_file('offer_html.html', html_offer)

re.sub('[a-zA-Z]' ,'',soup_offer.find_all('tr')[-3].find_all('td')[-1].text)[2:]

.find(class_='msg_footer')  #.find('td', string="Datums: ")
soup_offer.find('tr')

