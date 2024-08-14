import requests
import json
import pandas as pd
import time
from datetime import date, timedelta, datetime
import numpy as np
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, cast
from sqlalchemy import delete

#SETUP CONNECTION TO DATABASE
cnx = mysql.connector.connect(
    host="HOST",
    user="USER",
    password="PASSWORD",
    database="DB"
)

engine = create_engine("mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

#INPUT THE FIXUTRE DATE YOU WANT TO EXTRACT FROM THE ENPOINT, CLEAN IT AND STORE IT IN A DATAFRAME
date = '2024-07-17'

#CALCULATE THE A DATE 7 DAY LATER, THIS FIXTURE DATE WILL BE EXTRACTED TOO
date_dt = datetime.strptime(date, '%Y-%m-%d')
new_date = date_dt + timedelta(days=7)
date1 = new_date.strftime('%Y-%m-%d')

#EXTRACT THE INFO FROM THE FIRST DATE
url = 'https://v3.football.api-sports.io/fixtures/?date='+date
apikey = 'YOUR_API_KEY'

payload = {}
headers= {'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': apikey}

response = requests.request("GET", url, headers=headers, data = payload)
r1 = json.loads(response.text.encode('utf8'))
data = r1['response']

data=pd.json_normalize(data, sep='_')
ongoing = data
ongoing.rename(columns = {'event_date':'event_date_UTC'}, inplace = True)
ongoing['query_time']=pd.Timestamp.today(tz='America/Mexico_City').strftime('%Y-%m-%dT%H:%M:%S')
ongoing['event_date']=pd.to_datetime(ongoing['fixture_timestamp'], unit='s',utc=True)
ongoing['event_date']=ongoing.apply(lambda x : x['event_date'].tz_convert('America/Mexico_City').strftime('%Y-%m-%dT%H:%M:%S'),axis=1)

ongoing = ongoing.drop(['league_logo','teams_home_logo','teams_away_logo','league_flag'],axis=1)
ongoing.fillna('', inplace=True)

df = ongoing
df.replace('', np.nan, inplace=True)
df = df.dropna(subset=['score_fulltime_home'])

#CALCULATE A DATE 30 DAYS IN THE PAST
date_dt = datetime.strptime(date, '%Y-%m-%d')
new_date = date_dt + timedelta(days=-30)
date2 = new_date.strftime('%Y-%m-%d')

#RETRIEVE THE RECORDS IN THE DATABASE 30 DAYS AGO FROM THE FIRST DATE
query = "select * from fixtures where fixture_date>='"+date2+"'"
df1 = pd.read_sql(query, con=cnx)

#REMOVE DUPLICATE RECORDS USING FIXTURE_ID FIELD AS A KEY AND THEN CLEAN THE DATAFRAME 
df3 = df[~df['fixture_id'].isin(df1['fixture_id'])]
df3.replace('', np.nan, inplace=True)
df4 = df3.dropna(subset=['score_fulltime_home'])
duplicates = df4[df4.duplicated('fixture_id', keep=False)]
df4 = df4.reset_index()
del df4['index']

#INSERT INTO THE DATABASE THE RESULTING DATAFRAME
j=0
i=10000

while j<len(df):
  df5 = df4[j:i]
  print(i)
  df5.to_sql(name='fixtures', con=engine, if_exists='append', index=False)
  j=i
  i=j+10000
  print(i)

#DELETE FROM THE DATABASE THE RECORDS OF THE FIRST DATE IN THE TABLE NEXT_FIXTURES
mycursor = cnx.cursor()
sql = "DELETE FROM next_fixtures WHERE fixture_Date = '"+date+"'"
mycursor.execute(sql)
cnx.commit()

#REPEAT THE PROCESS FOR THE SECOND DATE, BUT NOW IT WILL STORED IN THE TABLE NEXT_FIXTURES
url = 'https://v3.football.api-sports.io/fixtures/?date='+date1
apikey = 'YOUR_API_KEY'

payload = {}
headers= {'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': apikey}

response = requests.request("GET", url, headers=headers, data = payload)
r1 = json.loads(response.text.encode('utf8'))
data = r1['response']

data=pd.json_normalize(data, sep='_')
ongoing = data
ongoing.rename(columns = {'event_date':'event_date_UTC'}, inplace = True)
ongoing['query_time']=pd.Timestamp.today(tz='America/Mexico_City').strftime('%Y-%m-%dT%H:%M:%S')
ongoing['event_date']=pd.to_datetime(ongoing['fixture_timestamp'], unit='s',utc=True)
ongoing['event_date']=ongoing.apply(lambda x : x['event_date'].tz_convert('America/Mexico_City').strftime('%Y-%m-%dT%H:%M:%S'),axis=1)

ongoing = ongoing.drop(['league_logo','teams_home_logo','teams_away_logo','league_flag'],axis=1)
ongoing.fillna('', inplace=True)

df = ongoing
query = "select * from next_fixtures"
df1 = pd.read_sql(query, con=cnx)
df3 = df[~df['fixture_id'].isin(df1['fixture_id'])]
df3.replace('', np.nan, inplace=True)
df4 = df3
duplicates = df4[df4.duplicated('fixture_id', keep=False)]
df4 = df4.reset_index()
del df4['index']

j=0
i=10000

while j<len(df):
  df5 = df4[j:i]
  print(i)
  df5.to_sql(name='next_fixtures', con=engine, if_exists='append', index=False)
  j=i
  i=j+10000
  print(i)
