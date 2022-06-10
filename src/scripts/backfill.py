from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
import glob
import time

client = MongoClient('localhost', 27017)

all_comments = client.resilient.all

files = glob.glob('/shared/jackie/resilient-communities/removed/*.pkl')

files = files[files.index('/shared/jackie/resilient-communities/removed/worldnews.pkl'):]

for file in files:

    subreddit = file.split('/')[-1].split('.')[0]

    print(f'Storing r/{subreddit}.')

    removed_comments = pd.read_pickle(file)

    time.sleep(1)

    for comment in tqdm(removed_comments.itertuples(), total=len(removed_comments)):
        all_comments.update_one({'id': comment.id}, {'$set': {
            'removed': comment.removed,
            'requeried_utc': comment.retrieved_utc
        }})
