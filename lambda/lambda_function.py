import boto3
import pandas as pd
import transformation
import psycopg2
import os

rdsEndpoint = os.environ['endpoint']
rdsPort = os.environ['port']
rdsUser = os.environ['user']
rdsRegion = os.environ['region']
rdsDatabaseName = os.environ['database']
rdsPassword = os.environ['password']
johnHopkinsURL = os.environ['jh']
nytURL = os.environ['nyt']
snsARN = os.environ['sns']

def notify(text):
    try:
        sns = boto3.client('sns')
        sns.publish(TopicArn = snsARN, Message = text)
    except Exception as e:
        print("Not able to send SMS due to {}".format(e))
        exit(1)

def database_connection():
    try:
        conn = psycopg2.connect(host=rdsEndpoint, port=rdsPort, database=rdsDatabaseName, user=rdsUser, password=rdsPassword)
        return conn
    except Exception as e:
        notify("Database connection failed due to {}".format(e))
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
        row = (dfFinal.loc[dfFinal.shape[0]-days+i,'date'], int(dfFinal.loc[dfFinal.shape[0]-days+i,'cases']), int(dfFinal.loc[dfFinal.shape[0]-days+i,'deaths']),int(dfFinal.loc[dfFinal.shape[0]-days+i,'recovered']))
        data.append(row)
    records = ','.join(['%s'] * len(data))
    query = "insert into etl (reportdate,cases,deaths,recovered) values{}".format(records)
    return query,data
    
def lambda_handler(event, context):
    
    dfNYT = pd.read_csv(nytURL)
    dfJH = pd.read_csv(johnHopkinsURL,usecols=['Date','Country/Region','Recovered'])
    try:
        dfFinal = transformation.transform(dfNYT,dfJH)
    except Exception as e:
        notify("Transform function raised exception because {}",format(e))
        exit(1)
    conn = database_connection()
    cur = conn.cursor()
    data = []
    cur.execute("""SELECT to_regclass('etl')""")
    query_results = cur.fetchall()
    if query_results[0][0]==None:
        try:
            query = """CREATE TABLE etl (reportdate date PRIMARY KEY, cases integer, deaths integer, recovered integer)"""
            cur.execute(query)
        except Exception as e:
            notify("Exception raised while creation of table because {}".format(e))
            exit(1)
        try:
            query,data = first_insert(dfFinal,data)
            cur.execute(query,data)
        except Exception as e:
            notify("Couldn't complete first time data insertion in the table because {}".format(e))
            exit(1)
        notify("Table creation and first time data insertion done successfully")
    else:
        cur.execute("""SELECT max(reportdate) from etl""")
        query_results = cur.fetchall()
        diff = max(dfFinal['date']).date()-query_results[0][0]
        if diff.days>0:
            try:
                query,data = everyday_insert(dfFinal,data,diff.days)
                cur.execute(query,data)
            except Exception as e:
                notify("Daily insertion of data is unsuccessful because {}".format(e))
                exit(1)
            notify("Today "+str(diff.days)+" rows updated")
        else:
            notify("Nothing to update today, juthho ekdum")
    conn.commit()