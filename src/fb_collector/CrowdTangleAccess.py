#!/usr/bin/env python
# coding: utf-8

# # Collect Link Shares of Facebook Pages
# 
# We use the CrowdTangle API to collect posts by public Facebook pages.
# 
# __NOTE__: To use CrowdTangle, you must have created a list in CT with the pages/groups about which you care. Then you use the API to download data from that page/group.

# In[1]:


import time
import json
import requests

import pandas as pd


# In[ ]:





# In[3]:


domain = "api.crowdtangle.com"
token = "xxx"


# In[4]:


for year in [
    "2020",
]:

    print("Year:", year)

    req = requests.get("https://" + domain + "/posts", params={
        "token": token,
        "listIds": "XXXX", # List ID from CrowdTangle
        "startDate": "%s-01-01" % year,
        "endDate": "%d-01-01" % (int(year) + 1),
        "sortBy": "date",
        "count": 10000,
        "types": "photo,status,tweet,link,youtube",
    })

    print("Issuing first request...")
    response = req.json()
    print("Got first response...")

    with open("covid19_csr_%s_v2.json" % year, "w") as out_file:
        while "result" in response:

            print("Result Length:", len(response["result"]["posts"]))
            
            for post in response["result"]["posts"]:
                out_file.write("%s\n" % json.dumps(post))

            if ( "nextPage" in response["result"]["pagination"] ):
                out_file.flush()
                next_page = response["result"]["pagination"]["nextPage"]

                req = requests.get(next_page)
                
                try:
                    response = req.json()
                except Exception as e:
                    print("Error:", e)
                    response = {"status": 429}
                    
                if ( response["status"] == 429 ):
                    time.sleep(60)

                    req = requests.get(next_page)
                    response = req.json()
            else:
                break


# In[ ]:




