#!/usr/bin/env python
#!/usr/bin/env python
# coding: utf-8

# In[14]:


# coding=utf-8
import json
import requests
import pandas as pd
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

import sqlalchemy
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta


#provide access token
ACCESS_TOKEN = "#################"
PATH = "open_api/v1.2/reports/integrated/get/"
#set request parapmeters
Request_Params={
    "advertiser_id": "#################",
    "service_type": "AUCTION",
    "report_type": "AUDIENCE",
    "data_level": "AUCTION_ADVERTISER",
    "dimensions": [
         "advertiser_id",'province_id'
    ],
    "metrics": [
        "spend",
        "impressions",
        'clicks',
        'conversion_rate',
        "conversion",
        'cpc',
        'cpm'

        
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
    POSTGRES_PASSWORD = '"#################"!'
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
    print('start')
    #assign value to request variable 
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
    #construct request argument
    my_args = "{\"metrics\": %s, \"data_level\": \"%s\", \"end_date\": \"%s\",\"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"report_type\": \"%s\", \"page\": \"%s\", \"dimensions\": %s}" % (metrics, data_level, end_date, page_size, start_date, advertiser_id, service_type, report_type, page, dimensions)
    #get data and convert to dataframe
    out_json_data=get(my_args)
    df=pd.json_normalize(out_json_data['data']['list'])
    df['date']=start_date
    #df=df.set_index('date')

    df=df.rename(columns={"metrics.conversion": "conversion", "metrics.cost_per_1000_reached": "cost_per_1000_reached",
                         "metrics.cpm": "cpm", "metrics.cpc": "cpc","metrics.conversion_rate": "conversion_rate", 
                          "metrics.impressions": "impressions","metrics.spend": "spend", "dimensions.advertiser_id": "advertiser_id",
                         "metrics.reach": "reach", "metrics.clicks": "clicks", "dimensions.stat_time_hour": "stat_time_hour"})
    #convert all feature to specific type
    df['impressions']=pd.to_numeric(df['impressions'])
    df['dimensions.province_id']=pd.to_numeric(df['dimensions.province_id'])
    #append province id to the output data frame based on the province id
    f = open('geo_mapping.json', encoding="utf8")
    geo_mapping=json.load(f)
    geo_df = pd.json_normalize(geo_mapping['provinces'])
    df_merged=df.merge(geo_df[['id','name','country_code']], left_on='dimensions.province_id', right_on='id',how='inner')
    datatable='marketing_tiktok_audiencereport_dailygeo'
    uploader(df=df_merged,datatable=datatable)

    


# In[ ]:




