#!/usr/bin/env python
# coding: utf-8

# In[1]:

import sys
import time
import gzip
import json
import tweepy
import os.path
import pandas as pd

input_users = pd.read_csv(sys.argv[1])
print("Users to capture:", input_users.shape[0])

# Use the strings from your Twitter app webpage to populate these four 
# variables. Be sure and put the strings BETWEEN the quotation marks
# to make it a valid Python string.

consumer_key = "xxx"
consumer_secret = "xxx"
access_token = "xxx"
access_secret = "xxx"


# ### Connecting to Twitter
# Now we use the configured authentication information to connect
# to Twitter's API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Tweet from 3 January 2020
min_tweet_id = 1213164435730247682

for idx, row in input_users.iterrows():
    screenname = row["name"].lower().strip()
    print("Fetching:", screenname, "%d of %d" % (idx, input_users.shape[0]))

    if os.path.exists("%s.json" % screenname):
        print("\tAlready fetched.")
        continue

    with open("%s.json" % screenname, "w") as out_file:

        try:
            # Iterate through statuses
            for status in tweepy.Cursor(api.user_timeline, screen_name=screenname, tweet_mode="extended", since_id=min_tweet_id).items():
                out_file.write("%s\n" % json.dumps(status._json))
        except tweepy.TweepError as te:
            print('\tError!')
            print(te)
 


# In[ ]:
