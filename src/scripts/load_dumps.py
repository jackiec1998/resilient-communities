from matplotlib.pyplot import tick_params
import zstandard
import os
from pymongo import MongoClient
import json
import sys
import pandas as pd
import datetime as dt
import traceback
import time

client = MongoClient('localhost', 27017)
pushshift_comments = client.resilient.pushshift_comments
# pushshift_comments = client.resilient.test_comments

def read_lines_zst(file_name):
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
                yield line, file.tell()

            buffer = lines[-1]
        
        reader.close()

if __name__ == '__main__':

    print('Loading comments.')

    os.chdir('/shared/jackie/resilient-communities/dumps/')

    comments = []

    file_name = 'RC_2022-03.zst'
    file_size = os.path.getsize(file_name)
    
    bytes_processed = 0
    file_lines = 0
    bad_lines = 0

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
        'retrieved_on',
        'score',
        'stickied',
        'subreddit'
    ]

    # subreddits = ['UIUC']

    try:
        for line, bytes_processed in read_lines_zst(file_name):
            try:
                comment = json.loads(line)
                current = dt.datetime.fromtimestamp(int(comment['created_utc']))

                comment = {key: comment[key] for key in fields}
                while True:

                    attempts = 0

                    try:
                        pushshift_comments.update_one({'id': comment['id']}, 
                            {'$set': comment}, upsert=True)

                        # sys.exit()
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
                     f'{file_lines:,} lines read | '
                     f'{bad_lines:,} bad lines read | '
                     f'{(bytes_processed / file_size) * 100:.2f}%')

    except Exception as e:
        print(e)
        traceback.print_exc()
        sys.exit()
