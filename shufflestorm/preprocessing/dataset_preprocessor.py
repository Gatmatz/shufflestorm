import pandas as pd

def preload_bikes():
    files = ['data/bikedekho.csv', 'data/bikewale.csv']
    for file in files:
        df = pd.read_csv(file)
        df = df[['bike_name']].rename(columns={'bike_name': 'name'})
        df.insert(0, 'id', range(len(df)))
        df.to_csv(file, index=False)

def preload_cite_dblp():
    files = ['data/citeseer.csv']
    for file in files:
        df = pd.read_csv(file)
        df = df[['title']].rename(columns={'title': 'name'}).head(200000)
        df.insert(0, 'id', range(len(df)))
        df.to_csv(file, index=False)

preload_cite_dblp()
preload_bikes()