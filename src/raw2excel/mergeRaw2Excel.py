#!/usr/bin/env python
# coding: utf-8

import sys
import copy
import glob
import gzip
import json
import math
import time
import getopt
import os.path

import pandas as pd

from datetime import datetime

def print_usage():
    sys.stderr.write("USAGE: %s --facebook-dir=<dir> --twitter-dir=<dir>\n" % 
        sys.argv[0]
        )

optlist, args = getopt.getopt(
    sys.argv[1:], 
    'f:t:', 
    ['facebook-dir=', 'twitter-dir=']
    )
optmap = dict(optlist)

if "--facebook-dir" not in optmap or "--twitter-dir" not in optmap:
    print_usage()
    sys.exit(-1)

print("Reading:")
print("\tFacebook Directory:", optmap["--facebook-dir"])
print("\tTwitter Directory:", optmap["--twitter-dir"])

fb_collection_path = optmap["--facebook-dir"]
tw_collection_path = optmap["--twitter-dir"]

umd_post_id_numeric = 0
map_platform_post_id_to_umd_post_id = {}


def create_post_row():
    row_schema = {
        "UmdPostId": None,
        "AccountPlatformId": None,
        "PlatformPostID": None,
        "URL": None,
        "Text": None,
        "Platform": None,
        "TimestampDownload": None,
        "TimestampPosted": None,
        "FBLikes": None,
        "FBShares": None,
        "FBComments": None,
        "FBReactionsTotal": None,
        "FBrxHeart": None,
        "FBrxHaHa": None,
        "FBrxWow": None,
        "FBrxSad": None,
        "FBrxAngry": None,
        "TwFavoriteCount": None,
        "TwRetweetCount": None,
        
        # CB Added 20190909
        "PostType": None,
        "FBrxThankful": None,
    }
    
    return row_schema

account_map_fb = {}
fb_rows = []

# We process FB posts first to collect metrics
#  about the accounts.
for dataset in glob.iglob(fb_collection_path + os.sep + "*.json.gz"):
    print("Processing...", dataset)
    with gzip.open(dataset, "r") as in_file:
        for line_ in in_file:
            line = line_.decode("utf8")
            
            fb_post = json.loads(line)
            
            # Process the author
            post_author = fb_post["account"]
            fb_author_id = None
            if "platformId" in post_author:
                fb_author_id = post_author["platformId"]
            elif "id" in post_author:
                fb_author_id = post_author["id"]
                if type(fb_author_id) != str:
                    fb_author_id = "%d" % fb_author_id
                post_author["platformId"] = fb_author_id
            else:
                print("ERROR:", fb_post["platformId"])
                sys.exit(-1)
            
            if fb_author_id not in account_map_fb:
                post_author["TimestampDownload"] = datetime.strptime(
                    fb_post["updated"], "%Y-%m-%d %H:%M:%S")
                
                post_author["FBPostCount"] = 0
                post_author["FBComments"] = 0
                post_author["FBLikes"] = 0
                post_author["FBShares"] = 0
                post_author["FBAllReactions"] = 0
                account_map_fb[fb_author_id] = post_author
            else:
                post_author = account_map_fb[fb_author_id]
                
            # Process the post
            this_post_row = create_post_row()
            this_post_row["Platform"] = "Facebook"
            
            # Top-level info
            this_post_row["AccountPlatformId"] = post_author["handle"]
            this_post_row["PlatformPostID"] = str(fb_post["platformId"])
            this_post_row["URL"] = fb_post["postUrl"]

            # Pull the post ID if we have it
            if ( this_post_row["PlatformPostID"] in map_platform_post_id_to_umd_post_id ):
                this_post_row["UmdPostId"] = map_platform_post_id_to_umd_post_id[this_post_row["PlatformPostID"]]
            else:
                this_post_row["UmdPostId"] = umd_post_id_numeric
                umd_post_id_numeric += 1
            
            # Get type
            this_post_type = fb_post["type"]
            this_post_row["PostType"] = this_post_type
            
            # Get text of post
            this_post_text = ""
            if ( "title" in fb_post ):
                this_post_text += " " + fb_post["title"]
                
            if ( "message" in fb_post ):
                this_post_text += " " + fb_post["message"]

            if ( "description" in fb_post ):
                this_post_text += " " + fb_post["description"]
                
            if ( "expandedLinks" in fb_post ):
                for l in fb_post["expandedLinks"]:
                    this_post_text += " " + l["expanded"]
                
            if ( "media" in fb_post ):
                for l in fb_post["media"]:
                    this_post_text += " " + l["url"]
            
            this_post_row["Text"] = this_post_text
            
            # Get statistics of post
            stat_block = fb_post["statistics"]["actual"]
            this_post_row["FBrxHeart"] = stat_block["loveCount"]
            this_post_row["FBrxHaHa"] = stat_block["hahaCount"]
            this_post_row["FBrxWow"] = stat_block["wowCount"]
            this_post_row["FBrxSad"] = stat_block["sadCount"]
            this_post_row["FBrxAngry"] = stat_block["angryCount"]
            this_post_row["FBrxThankful"] = stat_block["thankfulCount"]
            this_post_row["FBComments"] = stat_block["commentCount"]
            this_post_row["FBShares"] = stat_block["shareCount"]
            this_post_row["FBLikes"] = stat_block["likeCount"]
            
            reactions_total = 0
            for k, v in this_post_row.items():
                if k.startswith("FBrx"):
                    reactions_total += v
            this_post_row["FBReactionsTotal"] = reactions_total
            
            # post times
            this_post_row["TimestampPosted"] = fb_post["date"]
            this_post_row["TimestampDownload"] = fb_post["updated"]
            
            this_download_time = datetime.strptime(fb_post["updated"], "%Y-%m-%d %H:%M:%S")
            if post_author["TimestampDownload"] < this_download_time:
                post_author["TimestampDownload"] = this_download_time
            
            # Increment account-level stats
            post_author["FBPostCount"] += 1
            post_author["FBComments"] += stat_block["commentCount"]
            post_author["FBLikes"] += stat_block["likeCount"]
            post_author["FBShares"] += stat_block["shareCount"]
            post_author["FBAllReactions"] += reactions_total
            
            # Done
            fb_rows.append(this_post_row)

fb_posts_df = pd.DataFrame(fb_rows)
fb_posts_df.to_csv("fb_structure_posts.csv", index=False, encoding="utf8")


######################################################
# Twitter Analysis

tw_rows = []

for dataset in glob.iglob(tw_collection_path + os.sep + "*.json.gz"):
    print("Processing...", dataset)
    with gzip.open(dataset, "r") as in_file:
        for line_ in in_file:
            line = line_.decode("utf8")
            
            tw_post = json.loads(line)
            
            # Process the author
            post_author_sn = tw_post["user"]["screen_name"]
                
            # Process the post
            this_post_row = create_post_row()
            this_post_row["Platform"] = "Twitter"
            
            # Top-level info
            this_post_row["AccountPlatformId"] = post_author_sn
            this_post_row["PlatformPostID"] = tw_post["id_str"]
            this_post_row["URL"] = "https://twitter.com/%s/status/%d" % (post_author_sn, tw_post["id"])

            # Pull the post ID if we have it
            if ( this_post_row["PlatformPostID"] in map_platform_post_id_to_umd_post_id ):
                this_post_row["UmdPostId"] = map_platform_post_id_to_umd_post_id[this_post_row["PlatformPostID"]]
            else:
                this_post_row["UmdPostId"] = umd_post_id_numeric
                umd_post_id_numeric += 1
            
            # Get type
            this_post_type = "tweet"

            if "retweeted_status" in tw_post:
                this_post_type = "retweet"

            if tw_post["is_quote_status"] == True:
                this_post_type = "quote"

            if tw_post["in_reply_to_status_id"] is not None:
                this_post_type = "reply"

            this_post_row["PostType"] = this_post_type
            
            # Get text of post
            this_post_row["Text"] = tw_post["full_text"]
            
            # Get statistics of post
            this_post_row["TwFavoriteCount"] = tw_post["favorite_count"]
            this_post_row["TwRetweetCount"] = tw_post["retweet_count"]
            
            # post times
            this_post_row["TimestampPosted"] = tw_post["created_at"]
            this_post_row["TimestampDownload"] = datetime.now().strftime('%a %b %d %H:%M:%S +0000 %Y')
            
            
            # Done
            tw_rows.append(this_post_row)

tw_posts_df = pd.DataFrame(tw_rows)
tw_posts_df.to_csv("tw_structure_posts.csv", index=False, encoding="utf8")



merged_df = pd.concat([fb_posts_df, tw_posts_df])
merged_df.to_csv("all_structure_posts.csv", index=False, encoding="utf8")


