#!/usr/bin/env python
#!/usr/bin/env python
# coding: utf-8

# In[1]:


# coding=utf-8
import json
import requests
import pandas as pd
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

import datetime
import logging


import pandas as pd
from dateutil.relativedelta import relativedelta
import numpy as np
import datetime as dt
from datetime import timedelta, datetime
import sqlalchemy
from sqlalchemy import create_engine

import json
import requests
import pandas as pd
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

from datetime import date
from datetime import timedelta
#provide access
ACCESS_TOKEN = ""#################""
PATH = "open_api/v1.2/reports/integrated/get/"
# set request parameters
Request_Params={
    "advertiser_id": "#################",
    "service_type": "AUCTION",
    "report_type": "BASIC",
    "data_level": "AUCTION_ADVERTISER",
    "dimensions": [
         "advertiser_id",'stat_time_hour'
    ],
    "metrics": [
        "spend",
        "impressions",
        'clicks',
        "reach",
        "conversion",
        'cpc',
        'cpm',
        'cost_per_1000_reached',
        "conversion_rate",
        'cost_per_conversion',
        'frequency',
        'video_play_actions',
        'video_watched_2s',
        'video_watched_6s',
        'average_video_play',
        'average_video_play_per_user',
        'video_views_p25',
        'video_views_p50',
        'video_views_p75',
        'video_views_p100'
        
        
        
    ],
    "start_date": (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d'),
    "end_date": (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d'), 
    "page": 1,
    "page_size": 200
}


def build_url(path, query=""):
    # type: (str, str) -> str
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "ads.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get(json_str):
    # type: (str) -> dict
    """
    Send GET request
    :param json_str: Args in JSON format
    :return: Response in JSON format
    """
    args = json.loads(json_str)
    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN,
    }
    rsp = requests.get(url, headers=headers)
    return rsp.json()
def uploader(df,datatable):
    #credentials
    POSTGRES_ADDRESS = 'db.panoply.io'
    POSTGRES_PORT = '5439'
    POSTGRES_USERNAME = '"#################"'
    POSTGRES_PASSWORD = '"#################"'
    POSTGRES_DBNAME = '"#################"'
    #data base form
    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.
                    format(username=POSTGRES_USERNAME, 
                        password=POSTGRES_PASSWORD, 
                        ipaddress=POSTGRES_ADDRESS, 
                        port=POSTGRES_PORT, 
                        dbname=POSTGRES_DBNAME))
    #connect and creat engine
    cnx = create_engine(postgres_str)
    #inject dataframe to datatable
    df.to_sql(datatable, con=cnx, if_exists='append',index=False) 
if __name__ == '__main__':
    #parse parameter to variables
    metrics_list = Request_Params['metrics']
    metrics = json.dumps(metrics_list)
    data_level = Request_Params['data_level']
    page_size = Request_Params['page_size']
    start_date = Request_Params['start_date']
    end_date = Request_Params['end_date']
    advertiser_id = Request_Params['advertiser_id']
    service_type = Request_Params['service_type']
    report_type = Request_Params['report_type']
    page = Request_Params['page']
    dimensions_list = Request_Params['dimensions']
    dimensions = json.dumps(dimensions_list)
    # Args in JSON format
    my_args = "{\"metrics\": %s, \"data_level\": \"%s\", \"end_date\": \"%s\",\"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"report_type\": \"%s\", \"page\": \"%s\", \"dimensions\": %s}" % (metrics, data_level, end_date, page_size, start_date, advertiser_id, service_type, report_type, page, dimensions)
    out_json_data=get(my_args)
    #convert json to dataframe
    df=pd.json_normalize(out_json_data['data']['list'])
    df=df.rename(columns={"metrics.conversion": "conversion", "metrics.cost_per_1000_reached": "cost_per_1000_reached",
                         "metrics.cpm": "cpm", "metrics.cpc": "cpc","metrics.conversion_rate": "conversion_rate", 
                          "metrics.impressions": "impressions","metrics.spend": "spend", "dimensions.advertiser_id": "advertiser_id",
                         "metrics.reach": "reach", "metrics.clicks": "clicks", "dimensions.stat_time_hour": "stat_time_hour"})
    df=df.reset_index().sort_values(by=['stat_time_hour'])
    datatable='marketing_tiktok_hourly_basicreport'
    #     upload to panopaly
    uploader(df=df,datatable=datatable)


# In[ ]:




