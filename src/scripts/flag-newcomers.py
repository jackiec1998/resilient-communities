from pymongo import MongoClient
import pymongo
from psaw import PushshiftAPI as psaw
import pandas as pd
import time
import os
import pickle
from tqdm import tqdm
import warnings
import sys
import datetime as dt

warnings.filterwarnings('ignore')

client = MongoClient('localhost', 27017)
popular_threads = client.resilient.popular_threads

psaw = psaw()

def get_popular_threads(filter={}, columns=[], n=None):
    
    # Adding index to the query projection.
    fields = {field: 1 for field in ['id'] + columns}
    
    cursor = popular_threads.find(filter, fields)
    
    if n is not None:
        cursor = cursor.limit(n)
    
    df = pd.DataFrame(cursor)
    
    if len(df) == 0:
        return df
    
    return df.set_index('id') \
        .drop(columns=['_id'])[columns]

def flag_newcomers(thread, memoize):

    query_authors = thread.authors.copy()
    fullname = 't3_' + thread.Index
    newcomers = []
    troublemakers = []

    # Remove the authors we already memoized.
    for author in query_authors:
        if (author, thread.subreddit) in memoize.keys():

            if memoize[(author, thread.subreddit)] == fullname:
                newcomers.append(author)

            query_authors.remove(author)

    # Query the authors remaining, split up in batches
    # because sending too many authors will halt the request.
    while len(query_authors) != 0:
        batch = query_authors[:200] # load
        query_authors = query_authors[200:] # remove

        while len(batch) != 0:

            while True:
                try:
                    comments = pd.DataFrame(psaw.search_comments(
                        author = batch,
                        subreddit = thread.subreddit,
                        filter = ['author', 'link_id', 'created_utc'],
                        sort = 'asc',
                        limit = 100
                    ))
                    break

                except KeyboardInterrupt:
                    print('Keyboard interrupt detected.')

                    with open('memoize.pkl', 'wb') as file:
                        pickle.dump(memoize, file)
                    
                    sys.exit()

                except Exception as e:
                    time.sleep(10)
                    continue

            if len(comments) == 0:
                troublemakers += batch
                break

            comments = comments.iloc[
                comments.groupby('author')['created_utc'].idxmin()
            ]

            # Loop through comments to find link_id associated with
            # the author's earliest comment.
            for comment in comments.itertuples():
                if comment.link_id == fullname:
                    newcomers.append(comment.author)

                # Memoize the link_id associated with the author's first
                # comment on a subreddit.
                if (author, thread.subreddit) not in memoize.keys():
                    memoize[(author, thread.subreddit)] = comment.link_id

                # After we have the link_id, remove them from the batch.
                batch.remove(comment.author)

    while True:
        try:
            popular_threads.update_one({'id': thread.Index}, {
                '$set': {
                    'newcomers': newcomers,
                    'num_newcomers': len(newcomers),
                    'troublemakers': troublemakers,
                    'num_troublemakers': len(troublemakers),
                    'flagged_newcomers_utc': int(time.time())
                }
            })
            break
        except pymongo.errors.AutoReconnect:
            time.sleep(2)
            continue

    return

if __name__ == '__main__':

    start = int(time.time())

    if os.path.isfile('memoize.pkl'):
        with open('memoize.pkl', 'rb') as file:
            memoize = pickle.load(file)
    else:
        memoize = {}

    threads = get_popular_threads(filter={
        'comments': {'$type': 'object'},
        'comments.0.id': {'$exists': False},
        'flagged_newcomers_utc': {'$exists': False}
    }, columns=['authors', 'subreddit'])

    for thread in tqdm(threads.itertuples(), total=len(threads)):
        flag_newcomers(thread, memoize)

    print(f'Script took: {dt.timedetla(seconds=int(time.time()) - start)}.')