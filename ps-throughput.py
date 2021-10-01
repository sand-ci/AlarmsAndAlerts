import threading
import time
import datetime
import pandas as pd
from functools import reduce, wraps
from datetime import datetime, timedelta
import numpy as np
from  scipy.stats import zscore

import utils.queries as qrs
import utils.helpers as hp
from data_objects.NodesMetaData import NodesMetaData

from alarms import alarms



def fixMissingMetadata(rowDf, idx):
    metadf = NodesMetaData(idx, dateFrom, dateTo).df
    df1 = pd.merge(metadf[['host', 'ip', 'site']], rowDf[[
                   'src', 'hash']], left_on='ip', right_on='src', how='right')
    df2 = pd.merge(metadf[['host', 'ip', 'site']], rowDf[[
                   'dest', 'hash']], left_on='ip', right_on='dest', how='right')
    df = pd.merge(df1, df2, on=['hash'], suffixes=(
        '_src', '_dest'), how='inner')
    df = df[df.duplicated(subset=['hash']) == False]

    df = df.drop(columns=['ip_src', 'ip_dest'])
    df = pd.merge(rowDf, df, on=['hash', 'src', 'dest'], how='left')
    
    return df.rename(columns={'site_src': 'src_site', 'site_dest': 'dest_site'})


def queryData(idx, dateFrom, dateTo):
    data = []
    # query in portions since ES does not allow aggregations with more than 10000 bins
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list)-1):
        data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

    return data


def getStats(df, threshold):
    metaDf = fixMissingMetadata(df, 'ps_throughput')
    # convert to MB
    metaDf['value'] = round(metaDf['value']*1e-6)
    
    # split the data in 3 days
    sitesDf = metaDf.groupby(['src_site', 'dest_site',pd.Grouper(key='dt', freq='4d')])['value'].mean().to_frame().reset_index()
    
    # get the statistics
    sitesDf['z'] = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: round((x - x.mean())/x.std(),2))
    stdDf = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: x.std()).to_frame().reset_index().rename(columns={'value':'std'})
    stdDf['mean'] = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: x.mean()).values

    sitesDf = pd.merge(sitesDf, stdDf, left_on=['src_site','dest_site'], right_on=['src_site','dest_site'], how='left')

    #     get the % change with respect to the average for the period
    sitesDf['%change'] = round(((sitesDf['value'] - sitesDf['mean'])/sitesDf['mean'])*100)

    # grap the last 3 days period
    last3days = pd.to_datetime(max(sitesDf.dt.unique())).strftime("%Y-%m-%d")

    #     return only sites having significant drop in values in the most recent period
    return sitesDf[((sitesDf['z']<=-threshold)|(sitesDf['z']>=threshold))&(sitesDf['dt']==last3days)].rename(columns={'value':'last3days_avg'}).round(2)


def createMsg(vals, alarmType):
    msg = ''
    if alarmType =='Throughput failed':
        msg = f"Throughput measures failed between sites {vals['src_site']} and {vals['dest_site']}."

    elif alarmType == 'Throughput failed from/to multiple sites':
        msg = f"Throughput measures failed for {vals['site']} to sites: {vals['dest_sites']} and from sites: {vals['src_sites']}."

    elif alarmType == 'Bandwidth decreased':
        msg = f"Bandwidth decreased between sites {vals['src_site']} and {vals['dest_site']}. Current throughput is {vals['last3days_avg']} MB, dropped by {vals['%change']}% with respect to the 21-day-average."

    elif alarmType == 'Bandwidth decreased from/to multiple sites': 
        msg = f"Bandwidth decreased for site {vals['site']} to sites: {vals['dest_sites']} change: {[f'{v}%' for v in vals['dest_change']]}; and from sites: {vals['src_sites']}, dropped by {[f'{v}%' for v in vals['src_change']]} with respect to the 21-day-average."

    elif alarmType == 'Bandwidth increased':
        msg = f"Bandwidth increased between sites {vals['src_site']} and {vals['dest_site']}. Current throughput is {vals['last3days_avg']} MB, increased by {vals['%change']}% with respect to the 21-day average."

    elif alarmType == 'Bandwidth increased from/to multiple sites':
        msg = f"Bandwidth increased for site {vals['site']} to sites: {[f'{v}%' for v in vals['dest_change']]} change: {[f'+{v}%' for v in vals['dest_change']]}; and from sites: {vals['src_sites']}, increased by {[f'{v}%' for v in vals['src_change']]} with respect to the 21-day average."

    return msg


def createAlarms(alarmsDf, alarmType, minCount=5):
    # we aim for exposing a single site which shows significant change in throughput from/to 5 (default value) other sites in total
    # below we find the total count of unique sites related to a single site name
    src_cnt = alarmsDf[['src_site']].value_counts().to_frame().reset_index().rename(columns={0:'cnt', 'src_site': 'site'})
    dest_cnt = alarmsDf[['dest_site']].value_counts().to_frame().reset_index().rename(columns={0:'cnt', 'dest_site': 'site'})
    cntDf = pd.concat([src_cnt, dest_cnt]).groupby(['site']).sum().reset_index()
    
    for site in cntDf[cntDf['cnt']>=minCount]['site'].values:

        subset = alarmsDf[(alarmsDf['src_site']==site)|(alarmsDf['dest_site']==site)]
        
        # build the lists of values
        src_sites, dest_sites, src_change, dest_change = [],[],[],[]
        for idx, row in subset.iterrows():
            if row['src_site'] != site:
                src_sites.append(row['src_site'])
                src_change.append(row['%change'])
            if row['dest_site'] != site:
                dest_sites.append(row['dest_site'])
                dest_change.append(row['%change'])
        
        # create the alarm source content
        doc = {'dest_sites':dest_sites, 'dest_change':dest_change, 'src_sites':src_sites, 'src_change':src_change}
        doc['site'] = site

        # send the alarm and the correct message
        alarmOnMulty.addAlarm(body=createMsg(doc, f'{alarmType} from/to multiple sites'), tags=[site], source=doc)

        # delete the rows for which alarms were created
        alarmsDf = alarmsDf.drop(subset.index.values)


    # The rest will be send as 'regular' src-dest alarms
    for doc in alarmsDf[(alarmsDf['%change']<=-50)|(alarmsDf['%change']>=50)][['src_site', 'dest_site', 'last3days_avg', '%change']].to_dict('records'):
        alarmOnPair.addAlarm(body=createMsg(doc, alarmType), tags=[doc['src_site'], doc['dest_site']], source=doc)


now = datetime.utcnow()
dateTo = datetime.strftime(now, '%Y-%m-%d %H:%M')
dateFrom = datetime.strftime(now - timedelta(days=21), '%Y-%m-%d %H:%M')

# get the data
rowDf = pd.DataFrame(queryData('ps_throughput', dateFrom, dateTo))
rowDf['dt'] = pd.to_datetime(rowDf['from'], unit='ms')

# calculate the statistics
statsDf = getStats(rowDf, 1.9)


# Throughput measures failure
createAlarms(statsDf[statsDf['last3days_avg']==0], 'Throughput measures failure')
# Bandwidth decreased
createAlarms(statsDf[(statsDf['z']<=-1.9)], 'Bandwidth decreased')
# Bandwidth recovery
createAlarms(statsDf[(statsDf['z']>=1.9)], 'Bandwidth increased')