import pandas as pd

def transform(dfNYT,dfJH):
    dfJH = dfJH[dfJH['Country/Region']=='US'].drop(columns='Country/Region')
    dfJH.columns = ['date','recovered']
    dfNYT['date'] = pd.to_datetime(dfNYT['date'],format='%Y-%m-%d')
    dfJH['date'] = pd.to_datetime(dfJH['date'],format='%Y-%m-%d')
    dfNYT.set_index('date', inplace=True)
    dfJH.set_index('date',inplace=True)
    dfJH['recovered'] = dfJH['recovered'].astype('int64')
    dfFinal = dfNYT.join(dfJH, how='inner')
    dfFinal.reset_index(inplace=True)
    return dfFinal