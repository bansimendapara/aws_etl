import boto3
import pandas as pd
import transformation

def lambda_handler(event, context):
    johnHopkinsURL = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    nytURL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    dfNYT = pd.read_csv(nytURL)
    dfJH = pd.read_csv(johnHopkinsURL,usecols=['Date','Country/Region','Recovered'])
    dfFinal = transformation.transform(dfNYT,dfJH)
    