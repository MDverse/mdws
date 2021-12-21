import pandas as pd
import matplotlib.pyplot as plt

# Data opening

datasets = pd.read_csv("datasets.tsv", sep="\t")
files = pd.read_csv("files.tsv", sep="\t")

tab = pd.merge(datasets, files, on=["dataset_id", "origin"])

# Analysis


fig = plt.figure()

## File size


hist = tab["file_size"].hist(range=[tab["file_size"].min(), tab["file_size"].max()])
plt.xlabel("File size")
plt.ylabel("Number of files")
plt.savefig("histogram_files_size.png")
plt.show()

## Number of files per filetype

count_filetype = tab["file_type"].value_counts().plot(kind="bar")
plt.show()

## Number of files submitted per year

date = list(tab["date_creation"])
years = []
for i in range(len(date)):
    years.append(date[i][:4])
years.sort()
plt.hist(years)
plt.show()
