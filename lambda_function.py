import json
import boto3
import pandas as pd
import transformation
import psycopg2
import sys
import time
import os

rdsEndpoint = os.environ['endpoint']
rdsPort = os.environ['port']
rdsUser = os.environ['user']
rdsRegion = os.environ['region']
rdsDatabaseName = os.environ['database']
rdsPassword = os.environ['password']
johnHopkinsURL = os.environ['jh']
nytURL = os.environ['nyt']
    
def database_connection():
    try:
        conn = psycopg2.connect(host=rdsEndpoint, port=rdsPort, database=rdsDatabaseName, user=rdsUser, password=rdsPassword)
    except Exception as e:
        print("Database connection failed due to {}".format(e))
        exit(1)

def first_insert(dfFinal,data):
    for i in dfFinal.index:
        row = (dfFinal.loc[i,'date'], int(dfFinal.loc[i,'cases']),int(dfFinal.loc[i,'deaths']),int(dfFinal.loc[i,'recovered']))
        data.append(row)
    records = ','.join(['%s'] * len(data))
    query = "insert into etl (reportdate,cases,deaths,recovered) values{}".format(records)
    return query,data

def everyday_insert(dfFinal,data,days):
    for i in range(days):
        row = (dfFinal.loc[dfFinal.shape[0]-diff.days+i,'date'], int(dfFinal.loc[dfFinal.shape[0]-diff.days+i,'cases']),int(dfFinal.loc[dfFinal.shape[0]-diff.days+i,'deaths']),int(dfFinal.loc[dfFinal.shape[0]-diff.days+i,'recovered']))
        data.append(row)
    records = ','.join(['%s'] * len(data))
    query = "insert into etl (reportdate,cases,deaths,recovered) values{}".format(records)
    return query,data
    
    
def lambda_handler(event, context):
    
    dfNYT = pd.read_csv(nytURL)
    dfJH = pd.read_csv(johnHopkinsURL,usecols=['Date','Country/Region','Recovered'])
    dfFinal = transformation.transform(dfNYT,dfJH)
    
    conn = database_connection()
    cur = conn.cursor()
    cur.execute("""SELECT count(reportdate) from etl""")
    query_results = cur.fetchall()
    
    data = []
    
    if query_results[0][0]==0:
        query,data = first_insert(dfFinal,data)
        cur.execute(query,data)
    else:
        cur.execute("""SELECT max(reportdate) from etl""")
        query_results = cur.fetchall()
        diff = max(dfFinal['date']).date()-query_results[0][0]
        query,data = everyday_insert(dfFinal,data,diff.days)
        cur.execute(query,data)