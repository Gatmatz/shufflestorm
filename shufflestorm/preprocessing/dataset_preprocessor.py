import pandas as pd

def preload_bikes():
    """Preprocesses the bike datasets by renaming columns and adding index IDs."""
    files = ['data/bikedekho.csv', 'data/bikewale.csv']
    for file in files:
        df = pd.read_csv(file)
        df = df[['bike_name']].rename(columns={'bike_name': 'name'})
        df.insert(0, 'id', range(len(df)))
        df.to_csv(file, index=False)

def preload_citeseer():
    """Preprocesses the CiteSeer dataset by extracting the top titles, renaming columns, and adding index IDs."""
    files = ['data/citeseer.csv']
    for file in files:
        df = pd.read_csv(file)
        df = df[['title']].rename(columns={'title': 'name'}).head(200000)
        df.insert(0, 'id', range(len(df)))
        df.to_csv(file, index=False)


preload_bikes()
preload_citeseer()