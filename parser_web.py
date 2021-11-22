import requests
from bs4 import BeautifulSoup
import csv

URL = "https://zenodo.org/record/4743386/preview/NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip"
r = requests.get(URL)
soup = BeautifulSoup(r.content, 'html5lib')
# print(soup.prettify())

quotes = []

table = soup.find('ul', attrs={'class': 'tree list-unstyled'})
print(table)

chaine = []
for row in table.findAll('span'):
    chaine.append(row.text)

for i in range(0, len(chaine), 2):
    quote = {}
    quote['filename'] = chaine[i]
    quote['extension'] = chaine[i][-3:]
    quote['size'] = chaine[i + 1]
    quotes.append(quote)

filename = 'files_zip.csv'
with open(filename, 'w', newline='') as f:
    w = csv.DictWriter(f, ['filename', 'extension', 'size'])
    w.writeheader()
    for quote in quotes:
        w.writerow(quote)
