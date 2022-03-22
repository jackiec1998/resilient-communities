'''
    To run this file, execute the following line:
        nohup python3 monitor-mongo.py > /dev/null 2>&1 &

    The line executes the script in the background and ignores 
    any output it gets.
    
    The script will log information in its respective log file
    in /logs/.    

    To find the process, execute the following line:
        ps aux | grep 'monitor-mongo.py'
'''

from pymongo import MongoClient
import yaml
from slack_sdk import WebClient
import os
import datetime as dt
import time
from dotenv import load_dotenv
import sys
import subprocess

load_dotenv()

os.chdir('../../')

open('logs/monitor-mongo.log', 'w').close()

def get_timestamp():
    return time.strftime('%x %I:%M:%S %p', time.localtime())

def post_message(message, channel='logging'):
    slack.chat_postMessage(
        channel = channel,
        text = message
    )

def log_message(message):
    with open('logs/monitor-mongo.log', 'a') as file:
        file.write(f'{get_timestamp()} | {message}\n')

def run(command):
    return subprocess.run(command.split(), stdout=subprocess.PIPE).stdout.decode('utf-8')

try:
    slack = WebClient(token=os.environ.get('SLACK_BOT_TOKEN'))
    post_message('Database monitoring is online!')
except Exception as e:
    log_message('Slack was not instantiated correctly. Exiting.')
    sys.exit()

log_message('Monitoring tasks.')

while True:

    # Check if the docker ps if it's running out container.
    if 'scuba-diver' not in run('docker ps'):
        log_message('Container not running. Attempting to restart.')

        # Check if the container exists.
        if 'scuba-diver' in run('docker ps -a'): # Container exists, restart.
            log_message('Container exists, restarting container.')

            run('docker start scuba-diver')

        else: # Container does not exist, rebuild.
            log_message('Container does not exist, rebuilding image.')
            
            run('docker run -d --name scuba-diver --restart=always ' \
                '-v /srv/data/shared/db:/data/db --network host mongo:latest')

        # Check processes again.
        if 'scuba-diver' in run('docker ps'):
            log_message('Restart successful. Container is running.')
        
        else: # Fatal error, message on Slack.
            post_message('Script unable to restart container. Database is offline.')
    
    else:
        log_message('Container is running.')
        time.sleep(1_800) # Sleep a half an hour.