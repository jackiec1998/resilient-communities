# Resilient Communities

Repository containing code that ingests, manages, and analyzes data for the community resilience project.

## Components

### Database

The database is a MongoDB instance hosted using Docker.

- There should be a script to deploy a fresh instance of the MongoDB database.

- There should be a script that monitors whether the MongoDB database is available. If not, attempt to redeploy and message on Slack.

### Data Collection

There are a variety of scripts ingesting data from Reddit.

- The `collect-all.py` script collects comments from r/all.

- The `collect-popular.py` script collects threads from r/popular.

The scripts output to their own respective log files in `/logs/`.