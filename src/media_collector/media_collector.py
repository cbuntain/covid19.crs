#!/usr/bin/python

import sys
import glob
import gzip
import json
import requests

import os
import os.path

class Message(object):

    def __init__(self, platform, author, msg_id):

        self.platform = platform
        self.author = author
        self.msg_id = msg_id
        self.media = []

def parse_fb_msg(fb_msg):

    msg_obj = None

    if "media" in fb_msg:

        msg_obj = Message(
            platform="facebook", 
            author=fb_msg["account"]["handle"], 
            msg_id=fb_msg["platformId"]
        )

        for m in fb_msg["media"]:
            if m["type"] == "photo":
                msg_obj.media.append(m["url"])

    return msg_obj

def parse_tw_msg(tw_msg):

    msg_obj = None
    
    if "extended_entities" in tw_msg:

        msg_obj = Message(
            platform="twitter", 
            author=tw_msg["user"]["screen_name"], 
            msg_id=tw_msg["id"]
        )

        for m in tw_msg["extended_entities"]["media"]:
            if m["type"] == "photo":
                msg_obj.media.append(m["media_url"])

    return msg_obj


def parse_msg(msg):

    if "platformId" in msg:
        return parse_fb_msg(msg)

    else:
        return parse_tw_msg(msg)


def download_media(image_msg):

    if not os.path.exists(image_msg.platform):
        os.mkdir(image_msg.platform)

    author_dir = image_msg.platform + os.sep + image_msg.author
    if not os.path.exists(author_dir):
        os.mkdir(author_dir)

    msg_dir = author_dir + os.sep + str(image_msg.msg_id)
    if not os.path.exists(msg_dir):
        os.mkdir(msg_dir)

    for idx, media_url in enumerate(image_msg.media):

        # Only download this URL if a file doesn't already
        #  exist for it
        if len(glob.glob(msg_dir + os.sep + str(idx) + ".*")) > 0:
            continue

        r = requests.get(media_url, allow_redirects=True)

        ctype = r.headers.get('content-type')
        extension = ctype.rpartition("/")[-1]
        image_path = msg_dir + os.sep + str(idx) + "." + extension
        print("\t", image_path)

        with open(image_path, 'wb') as img_file:
            img_file.write(r.content)







for in_file_path in sys.argv[1:]:
    print(in_file_path)

    with gzip.open(in_file_path) as in_file:

        for line in in_file:

            msg = json.loads(line)
            image_msg = parse_msg(msg)

            if image_msg is not None and len(image_msg.media) > 0:

                print(image_msg.platform, image_msg.author, image_msg.msg_id, len(image_msg.media))

                download_media(image_msg)

