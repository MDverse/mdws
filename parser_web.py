import requests
from bs4 import BeautifulSoup

URL = "https://zenodo.org/record/4743386/preview/NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip"
r = requests.get(URL)
soup = BeautifulSoup(r.content, 'html5lib')
# print(soup.prettify())

quotes = []

table = soup.find('ul', attrs={'class': 'tree list-unstyled'})
print(table)
