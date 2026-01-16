import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://zenodo.org/record/4743386/preview/NoPTM-2_Mix_CHARMM36m_0.1x3mks.zip"
r = requests.get(URL)
soup = BeautifulSoup(r.content, "html5lib")
# print(soup.prettify())

quotes = []

table = soup.find("ul", attrs={"class": "tree list-unstyled"})
print(table)

chaine = []
for row in table.findAll("span"):
    chaine.append(row.text)

for i in range(0, len(chaine), 2):
    quote = {}
    quote["filename"] = chaine[i]
    quote["extension"] = chaine[i][-3:]
    size = chaine[i + 1].split()
    if size[1] == "GB":
        quote["size"] = float(size[0]) * (10**9)
    elif size[1] == "MB":
        quote["size"] = float(size[0]) * (10**6)
    elif size[1] == "kB":
        quote["size"] = float(size[0]) * (10**3)
    else:
        quote["size"] = float(size[0])
    quotes.append(quote)

files_df = pd.DataFrame(quotes).set_index("filename")
files_df.to_csv("files_zip1.csv")
