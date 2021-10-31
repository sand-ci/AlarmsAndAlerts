'''
{
    "category": "Networking",
    "subcategory": "Perfsonar",
    "event": "source can't reach any",
    "description": "Code running once a day at UC k8s cluster, checks in ps_trace for issues with reaching a destination. Alarm is generated if host cannot reach any destination (destination_reched=False to all tested hosts). The code can be found here: https://github.com/sand-ci/AlarmsAndAlerts/blob/main/ps-trace.py. The tag field contains affected site name",
    "template": "Host(s) %{hosts} @ %{site} cannot reach any destination out of %{num_hosts_other_end} hosts"
}

{
    "category": "Networking",
    "subcategory": "Perfsonar",
    "event": "destination can't be reached from any",
    "description": "Code running once a day at UC k8s cluster, checks in ps_trace for issues with reaching a destination. Alarm is generated if host cannot be reached by any source (destination_reched=False from all hosts). The code can be found here: https://github.com/sand-ci/AlarmsAndAlerts/blob/main/ps-trace.py. The tag field contains affected site name",
    "template": "Host(s) %{hosts} @ %{site} cannot be reached by any source out of %{num_hosts_other_end} hosts"
}

{
    "category": "Networking",
    "subcategory": "Perfsonar",
    "event": "destination can't be reached from multiple",
    "description": "Code running once a day at UC k8s cluster, checks in ps_trace for issues with reaching a destination. Alarm is generated if host cannot be reached by >20 sources (destination_reched=False from >20 hosts). The code can be found here: https://github.com/sand-ci/AlarmsAndAlerts/blob/main/ps-trace.py. The tag field contains affected site name",
    "template": "Host(s) %{hosts} @ %{site} cannot be reached from c{cannotBeReachedFrom} out of %{totalNumSites} source sites: %{cannotBeReachedFrom}"
}

'''


import itertools
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager
import datetime as dt


import pandas as pd
from IPython.display import display
import numpy as np

from data_objects.NodesMetaData import NodesMetaData
import utils.helpers as hp
from utils.helpers import timer
from alarms import alarms

# the query
def queryPSTrace(dt, include=["timestamp","destination_reached","src","dest","looping","path_complete",'ipv6']):
    query = {
      "query" : {
        "bool" : {
          "must" : [
            {
              "range" : {
                "timestamp" : {
                  "gt" : dt[0],
                  "lte": dt[1]
                }
              }
            }
          ]
        }
      }
    }
#     print(str(query).replace("\'", "\""))
    try:
        return scan_gen(scan(hp.es,index="ps_trace",query=query, _source=include, filter_path=['_scroll_id', '_shards', 'hits.hits._source']))
    except Exception as e:
        print(e)


def scan_gen(scan):
    while True:
        try:
            yield next(scan)['_source']
        except:
            break

# get the data from ES
def ps_trace(dt):
    scan_gen = queryPSTrace(dt)
    items = []

    for meta in scan_gen:
        items.append(meta)

    return items


# create a shared variable to store the data
manager = Manager()
data = manager.list()

# query in chunks based on time ranges
def getTraceData(dtRange):
    traceData = ps_trace(dtRange)
    if len(traceData)>0:
        data.extend(traceData)

# laod the data in parallel
def run(dateFrom, dateTo):
    # query the past 24 hours and split the period into 8 time ranges
    dtList = hp.GetTimeRanges(dateFrom, dateTo, 8)
    with ProcessPoolExecutor(max_workers=4) as pool:
        result = pool.map(getTraceData, [[dtList[i], dtList[i+1]] for i in range(len(dtList)-1)])


def findAllDestinationsNeverReached(start, df, alarm, alarmType):
    end = 'dest' if start == 'src' else 'src'
    issuesDf = df.groupby(f'{start}_host').agg({'destination_reached':['sum', 'count'], f'{end}_host':'nunique'}).reset_index()
    issuesDf.columns = [' '.join(col).strip() for col in issuesDf.columns.values]

    issuesDf = pd.merge(metaDf[['site', 'host']], issuesDf, left_on='host', right_on=f'{start}_host', how='right').rename(columns={'site':f'{start}_site'}).drop_duplicates()
    issuesDf = issuesDf[(issuesDf[f'{end}_host nunique']>1)&~(issuesDf[f'{start}_site'].isnull())]
    nr = issuesDf[(issuesDf['destination_reached sum']==0)]

#     display(issuesDf)

    nrHosts = []
    for site, group in nr.groupby(f'{start}_site'):
        hosts = group.host.values
        nrHosts.extend(hosts)
        doc = {
                'hosts': list(hosts),
                'site': site,
                'num_hosts_other_end': int(nr[nr[f'{start}_site']==site][f'{end}_host nunique'].sum())
              }
#         print(doc)
#         print()
        alarm.addAlarm(body=f"{alarmType} host", tags=[site], source=doc)

    return nrHosts


def issuesWithMultipleSites(start, threshold, nrHosts, df, metaDf, alarm, alarmType):
    end = 'dest' if start == 'src' else 'src'
    # get the unique src-dest combinations and sum the destination_reached in order 
    # to find all pairs that never reached the destinarion
    aggBySrcDest = df.groupby(['src','dest']).agg({'destination_reached':['sum', 'count']}).reset_index()
    aggBySrcDest.columns = [' '.join(col).strip() for col in aggBySrcDest.columns.values]
    aggBySrcDest = pd.merge(metaDf[['ip', 'host', 'site']], aggBySrcDest, left_on='ip', right_on='src', how='right').rename(columns={'host':'src_host', 'site':'src_site'}).drop(columns='ip')
    aggBySrcDest = pd.merge(metaDf[['ip', 'host', 'site']], aggBySrcDest, left_on='ip', right_on='dest', how='right').rename(columns={'host':'dest_host', 'site':'dest_site'}).drop(columns='ip')

    # grab src-dest pairs which never reached the destination
    zeroGroups = aggBySrcDest[aggBySrcDest['destination_reached sum']==0].reset_index(drop=True)

    # remove rows with empty values
    zeroGroups = zeroGroups[~(zeroGroups['src_host'].isnull())&~(zeroGroups['dest_host'].isnull())]
    # count the number of never reached WRT one end only
    zeroGroupsCnt = zeroGroups.groupby(f'{start}_host')[[f'{end}_host']].count().rename(columns={f'{end}_host':'cnt_other_end'})
    # get the ones passing the threshold, i.e. hosts that cannot be reached from (or cannot reach) more than 20 hosts
    moreThanTwentyEnds = zeroGroupsCnt[zeroGroupsCnt['cnt_other_end']>threshold]
    # add sites and hosts
    moreThanTwentyEnds = pd.merge(metaDf[['site', 'host']], moreThanTwentyEnds, left_on='host', right_on=f'{start}_host', how='right').rename(columns={'site':f'{start}_site'}).drop_duplicates()

    # drop the hosts already reported as never reached (or the ones the cannot reach any host)
    reportHosts = [h for h in list(moreThanTwentyEnds['host'].unique()) if h not in nrHosts]
    # get the sites of the reportHosts
    reportSites = zeroGroups[zeroGroups[f'{start}_host'].isin(reportHosts)].groupby([f'{start}_site',f'{start}_host'])[f'{end}_site'].apply(list).to_frame().reset_index()
#     display(reportSites)

    # loop over the sites and create an alarm for each on the list
    for site, group in reportSites.groupby(f'{start}_site'):
        slist = [x for x in list(set().union(*group[f'{end}_site'])) if x is not None]
        totalNumSites = len(aggBySrcDest[aggBySrcDest[f'{start}_host'].isin(group[f'{start}_host'].values)][f'{end}_site'].drop_duplicates())

        hosts = group[f'{start}_host'].values
#         print(f"cannot be reached from {len(slist)} out of {totalNumSites} sites")
        doc = {
                'hosts': list(hosts),
                'site': site,
                'cannotBeReachedFrom': sorted(slist, key=str.casefold),
                'totalNumSites': totalNumSites
              }
#         print(doc)
#         print()

        alarm.addAlarm(body=f"{alarmType} host", tags=[site], source=doc)


dateFrom, dateTo = hp.defaultTimeRange(24)
# print(dateFrom, dateTo)
run(dateFrom, dateTo)
df = pd.DataFrame(list(data))

# get all necessary data for the nodes and fill it in the raw data from ps_trace in order to repair missing information
metaDf = NodesMetaData('ps_trace', dateFrom, dateTo).df
rawDf = pd.DataFrame(list(data))
df = rawDf[~(rawDf['src'].isnull())&(rawDf['src']!='')&~(rawDf['dest'].isnull())&(rawDf['dest']!='')]
df = pd.merge(metaDf[['ip', 'site', 'host', 'is_ipv6']], df, left_on='ip', right_on='src', how='right').rename(columns={'site':'src_site', 'host':'src_host'}).drop(columns=['ip'])
df = pd.merge(metaDf[['ip', 'site', 'host']], df, left_on='ip', right_on='dest', how='right').rename(columns={'site':'dest_site', 'host':'dest_host'}).drop(columns=['ip'])
df = df[~(df['src'].isnull())&(df['src']!='')&~(df['dest'].isnull())&(df['dest']!='')]

#create the alarm types
alarmDestHostsCantBeReachedFromAny = alarms("Networking", "Perfsonar", "destination can't be reached from any")
alarmSrcHostsCantReachAny = alarms('Networking', 'Perfsonar', "source can't reach any")
alarmDestCantBeReachedFromMulty = alarms('Networking', 'Perfsonar', "destination can't be reached from multiple")

# send alarms
DestHostsCantBeReachedFromAny =  findAllDestinationsNeverReached(start='dest', 
                                                                 df=df,
                                                                 alarm=alarmDestHostsCantBeReachedFromAny,
                                                                 alarmType="destination can't be reached from any")

SrcHostsCantReachAny = findAllDestinationsNeverReached(start='src',
                                                       df=df,
                                                       alarm=alarmSrcHostsCantReachAny,
                                                       alarmType="source can't reach any")

issuesWithMultipleSites(start='dest',
                        threshold=20, 
                        nrHosts=DestHostsCantBeReachedFromAny,
                        df=df,
                        metaDf=metaDf,
                        alarm=alarmDestCantBeReachedFromMulty,
                        alarmType="destination can't be reached from multiple")
# issuesWithMultipleSites(start='src', threshold=20, nrHosts=SrcHostsCantReachAny)