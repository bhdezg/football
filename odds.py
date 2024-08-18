import requests
import json
import pandas as pd
import time
from datetime import date, timedelta, datetime
import numpy as np
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, cast
from sqlalchemy import delete

cnx = mysql.connector.connect(
    host="HOST",
    user="USER",
    password="PASSWORD",
    database="DB"
)

engine = create_engine("mysql+pymysql://")

page = 1
full_res = []
pages_remaining = True
date = '2024-06-02'

apikey = 'b40ba0ec64314d51df33e1ab61c69e8a'
payload = {}
headers= {'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': apikey}

while pages_remaining:
    url = 'https://v2.api-football.com/odds/date/'+date+'/label/1?page='+str(page)
    res = requests.request("GET", url, headers=headers, data = payload)
    r1 = json.loads(res.text.encode('utf8'))
    full_res.extend(r1['api']['odds'])
    print('Pagina '+str(page)+' de '+str(r1['api']['paging']['total']))
    page += 1
    time.sleep(1)
    if page > r1['api']['paging']['total']:
        pages_remaining = False

data = full_res
data2=pd.json_normalize(data, record_path='bookmakers', meta='fixture',sep='_')
result_2 = data2.to_json(orient="records")
data2_json = json.loads(result_2)

data3=pd.json_normalize(data2_json, sep='_')
result_3 = data3.to_json(orient="records")
data3_json = json.loads(result_3)

data4=pd.json_normalize(data3_json, record_path='bets', meta=['fixture_fixture_id','bookmaker_id'],sep='_',errors='ignore')
result_4 = data4.to_json(orient="records")
data4_json = json.loads(result_4)

data5=pd.json_normalize(data4_json, record_path='values', meta=['fixture_fixture_id','bookmaker_id'],sep='_',errors='ignore')
data5 = data5.drop_duplicates()
data5['record_count'] = data5.groupby(['fixture_fixture_id', 'bookmaker_id'])['value'].transform('count')
data5a = data5[data5['record_count']==3]
data6 = data5a.pivot(index=['fixture_fixture_id','bookmaker_id'], columns='value', values='odd')
data7= data6.reset_index(drop=False, inplace=False)

data7['Away']=pd.to_numeric(data7['Away'])
data7['Draw']=pd.to_numeric(data7['Draw'])
data7['Home']=pd.to_numeric(data7['Home'])
data7['Favorite'] = data7[['Away','Home','Draw']].idxmin(axis=1)
data7['fixture_date']=date
data7.rename(columns={'fixture_fixture_id': 'fixture_id'}, inplace=True)

data7.to_sql(name='odds_matchwinner', con=engine, if_exists='append', index=False)

page = 1
full_res = []
pages_remaining = True

payload = {}
headers = {
  'x-rapidapi-host': 'v3.football.api-sports.io',
  'x-rapidapi-key': 'b40ba0ec64314d51df33e1ab61c69e8a'
}

while pages_remaining:
    url = 'https://v3.football.api-sports.io/odds?bet=5&date='+date+'&page='+str(page)
    res = requests.request("GET", url, headers=headers, data = payload)
    r1 = json.loads(res.text.encode('utf8'))
    full_res.extend(r1['response'])
    print('Pagina '+str(page)+' de '+str(r1['paging']['total']))
    page += 1
    time.sleep(10)
    if page > r1['paging']['total']:
        pages_remaining = False

rows = []
for item in full_res:
    fixture_id = item['fixture']['id']
    fixture_date = item['fixture']['date']
    for bookmaker in item['bookmakers']:
        bookmaker_id = bookmaker['id']
        odds_dict = {}
        for bet in bookmaker['bets']:
            for value in bet['values']:
                odds_dict[value['value'].replace(".", "_").replace("/", "_").replace(" ", "_").lower()] = value['odd']
        row = {'fixture_id': fixture_id, 'fixture_date': fixture_date, 'bookmaker_id': bookmaker_id, **odds_dict}
        rows.append(row)

df = pd.DataFrame(rows)

df = df[['fixture_id', 'fixture_date', 'bookmaker_id',
         'over_0_5', 'under_0_5', 'over_1_5', 'under_1_5',
         'over_2_5', 'under_2_5', 'over_3_5', 'under_3_5',
         'over_4_5', 'under_4_5', 'over_5_5', 'under_5_5']]

df.to_sql(name='odds_overunder', con=engine, if_exists='append', index=False)
