import zstandard
import os
from pymongo import MongoClient
import json
import sys
import datetime as dt
import traceback
import time
import pickle

client = MongoClient('localhost', 27017)
collection = client.resilient.comments
popular_threads = client.resilient.popular_threads

# TODO: Pick up where you last left by adding an incrementor
# in the read_lines_zst() function.

def read_lines_zst(file_name):

    total_lines = 0

    with open(file_name, 'rb') as file:

        buffer = ''
        reader = zstandard.ZstdDecompressor(
            max_window_size = 2**31
        ).stream_reader(file)

        while True:

            chunk = reader.read(2**27).decode('utf-8')
            # chunk = reader.read(2**27).decode('iso-8859-1')

            if not chunk:
                break

            lines = (buffer + chunk).split('\n')

            for line in lines[:-1]:
                total_lines += 1

                yield line, file.tell(), line_number

            buffer = lines[-1]
        
        reader.close()

if __name__ == '__main__':

    print('Loading comments.')

    start = int(time.time())

    os.chdir('/shared/jackie/resilient-communities/dumps/')

    file_name = 'RC_2021-06.zst'
    last_line = 0
    file_size = os.path.getsize(file_name)
    
    bytes_processed = 0
    file_lines = 0
    bad_lines = 0
    skipped_lines = 0

    fields = [
        'author',
        'body',
        'created_utc',
        'edited',
        'id',
        'is_submitter',
        'link_id',
        'parent_id',
        'permalink',
        'retrieved_utc',
        'score',
        'stickied',
        'subreddit'
    ]

    pickle_file = file_name.split('.')[0] + '.pkl'

    if os.path.isfile(pickle_file):
        with open(pickle_file, 'rb') as file:
            subreddits_loaded = pickle.load(file)
    else:
        subreddits_loaded = set()

    subreddits = set(popular_threads.distinct('subreddit'))

    subreddits -= subreddits_loaded

    subreddits = list(subreddits)

    if not subreddits:
        print('No subreddits to filter through.')
        sys.exit()

    try:
        for line, bytes_processed, line_number in read_lines_zst(file_name):

            if line_number <= last_line:
                continue

            try:
                comment = json.loads(line)
                current = dt.datetime.fromtimestamp(int(comment['created_utc']))

                if comment['subreddit'] not in subreddits:
                    skipped_lines += 1
                    continue

                if 'retrieved_on' in comment.keys():
                    comment['retrieved_utc'] = comment['retrieved_on']

                comment = {key: comment[key] for key in fields}

                comment['source'] = file_name
                comment['removed'] = comment['body'] == '[removed]'
                comment['deleted'] = comment['body'] == '[deleted]'
                comment['loaded_utc'] = int(time.time())

                while True:

                    attempts = 0

                    try:
                        collection.update_one({'id': comment['id']}, 
                            {'$set': comment}, upsert=True)
                        break
                    except Exception as e:
                        attempts += 1
                        if attempts >= 10:
                            bad_lines += 1
                            break
                        time.sleep(2)
                        continue
                
            except (KeyError, json.JSONDecodeError) as e:
                bad_lines += 1

            file_lines += 1

            if file_lines % 10_000 == 0:
                date_format = '%Y-%m-%d %H:%M:%S'
                print(f'{current.strftime(date_format)} | '
                     f'{file_lines:,} read | '
                     f'{bad_lines:,} bad | '
                     f'{skipped_lines:,} skipped | '
                     f'{(bytes_processed / file_size) * 100:.2f}% | '
                     f'{dt.timedelta(seconds=int(time.time()) - start)}')

    except Exception as e:
        print(e)
        traceback.print_exc()
        sys.exit()

    subreddits_loaded = subreddits_loaded.union(set(subreddits))

    with open(pickle_file, 'wb') as file:
        pickle.dump(subreddits_loaded, file)

    str_format = '%Y-%m-%d %H:%M:%S'

    print(f'Completed @ {dt.datetime.now().strftime(str_format)} | {file_lines:,} lines read.')    
    print(f'Duration: {dt.timedelta(seconds=int(time.time()) - start)}.')