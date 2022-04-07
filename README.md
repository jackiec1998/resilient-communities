# Resilient Communities

Repository containing code that ingests, manages, and analyzes data for the community resilience project.

## Data

Data is managed by MongoDB instances hosted on Docker. The `collect-all.py` and `collect-popular.py` scripts (located in `/src/scripts/`) request r/all comments and r/popular threads, respectively, and input them into the database. Performance information is logged in its respectively file in `/logs/`. Additionally, each script messages Slack every six hours with updates on data collection.

Due to previous database reliability issues, there's an additional script that monitors whether the MongoDB Docker container is running every thirty minutes. If there's a down period, the script will try and restart the container, but if it's unable to restart it then a message will be sent to Slack.

### r/popular Threads

The *top 100 r/popular threads* are collected every two minutes where an individual r/popular thread is *represented by a series of snapshots throughout time.* This allows us to track changes throughout time, e.g., the number of upvotes a r/popular thread has and its position on the r/popular feed (i.e., is it the fifth r/popular thread or further down?).

Here's a bulletpoint list of notable attributes we can derive from r/popular threads that will be useful in our subsequent analysis. Asterisks denote that the feature hasn't been coded yet.

- `created_to_popular:` The number of seconds between thread creation, `created_utc`, to the first snapshot retrieval time.

    - *Note: Because we collect r/popular threads every two minutes, it's going to be off a few seconds.*

- `active_window:` The number of seconds between the first comment on an r/popular thread and the last comment on an r/popular thread.

    - *Note: This is based off of r/all comment stream, not Pushshift data.*

- `popular_window:` The number of seconds the r/popular thread was on r/popular, specifically the top 100.

- `num_snapshots:` The number of snapshots, or data points on a time series, we have of an individual r/popular thread.

- `*num_newcomers/newcomers:` The number of newcomers, or who those newcomers are, in an r/popular thread. *Newcomers are defined as users who have never posted within the community since January 1, 2020.* For example, u/happy_dolphin is labeled a newcomer to r/aquariums because they don't have an earlier comment on r/aquariums, ignoring comments that occur before January 1, 2020.

    - `*percent_newcomers:` The percentage of authors in a thread that are new to the community.

    - *Note: This requires us to collect historical data from Pushshift for the communities we want to analyze.*

- `*newcomer_retention:` The number of hours until 90% of the newcomer (or some other threshold) no longer contribute to the community.  

- `*num_removals/removals:` The number of removed comments, or who those removed authors are, in an r/popular thread. We're able to capture the content of some removed comments because we streamed them into the database before a moderator intervention was imposed.

    - `*percent_removals:` The percentage of comments that were removed in a thread.

    - *Note: This requires a requery for comments because we need to check whether the comment has been removed.*

- `num_comments:` The number of comments on an r/popular thread.

    - *Note: This count can either come from the r/all comment stream or from Pushshift. Our r/all comment stream drops some comments that Pushshift captures through requesting IDs sequentially.*

- `*num_newcomers_removed:` The number of newcomers that were removed.

    - `*percent_newcomers_removed:` The percentage of newcomers that were removed.

- `*peak_rank:` The highest rank an r/popular reaches in its lifespan.

## Analysis

- `num_comments ~ percent_newcomers:` Is there any relation to the prevalence of newcomers to the number of comments an r/popular receives?

- `peak_rank ~ percent_newcomers:` Does the percentage of newcomers have any relation to how high an r/popular thread reaches?

- `percent_removal ~ percent_newcomers:` Does the percentage of newcomers have any relation to the percentage of removed comments in an r/popular thread?

Still haven't used `active_window`, `popular_window`, `created_to_popular` as indepdenent or dependent variables. 