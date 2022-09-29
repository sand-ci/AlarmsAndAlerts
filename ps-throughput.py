import datetime
import pandas as pd
from datetime import datetime, timedelta
import hashlib

import utils.queries as qrs
import utils.helpers as hp

from alarms import alarms



def query4Avg(dateFrom, dateTo):
    query = {
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "gt" : dateFrom,
                      "lte": dateTo
                    }
                  }
                },
                {
                  "term" : {
                    "src_production" : True
                  }
                },
                {
                  "term" : {
                    "dest_production" : True
                  }
                }
              ]
            }
          }
              
    aggregations = {
                "groupby" : {
                  "composite" : {
                    "size" : 9999,
                    "sources" : [
                      {
                        "ipv6" : {
                          "terms" : {
                            "field" : "ipv6"
                          }
                        }
                      },
                      {
                        "src" : {
                          "terms" : {
                            "field" : "src"
                          }
                        }
                      },
                      {
                        "dest" : {
                          "terms" : {
                            "field" : "dest"
                          }
                        }
                      },
                      {
                        "src_host" : {
                          "terms" : {
                            "field" : "src_host"
                          }
                        }
                      },
                      {
                        "dest_host" : {
                          "terms" : {
                            "field" : "dest_host"
                          }
                        }
                      },
                      {
                        "src_site" : {
                          "terms" : {
                            "field" : "src_site"
                          }
                        }
                      },
                      {
                        "dest_site" : {
                          "terms" : {
                            "field" : "dest_site"
                          }
                        }
                      }
                    ]
                  },
                  "aggs": {
                    "throughput": {
                      "avg": {
                        "field": "throughput"
                      }
                    }
                  }
                }
              }
            


#     print(idx, str(query).replace("\'", "\""))
    aggrs = []

    aggdata = hp.es.search(index='ps_throughput', query=query, aggregations=aggregations)
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({'hash': str(item['key']['src']+'-'+item['key']['dest']),
                      'from':dateFrom, 'to':dateTo,
                      'ipv6': item['key']['ipv6'],
                      'src': item['key']['src'], 'dest': item['key']['dest'],
                      'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                      'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
                      'value': item['throughput']['value'],
                      'doc_count': item['doc_count']
                     })

    return aggrs


# Fill in hosts and site names where missing by quering the ps_alarms_meta index
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf['pair'] = rawDf['src']+rawDf['dest']
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
                columns={'host':'host_src','site':'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
                columns={'host':'host_dest','site':'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['src_site'].fillna(rawDf.pop('site_src'))
    rawDf['dest_site'] = rawDf['dest_site'].fillna(rawDf.pop('site_dest'))
    rawDf['src_host'] = rawDf['src_host'].fillna(rawDf.pop('host_src'))
    rawDf['dest_host'] = rawDf['dest_host'].fillna(rawDf.pop('host_dest'))

    return rawDf


def queryData(dateFrom, dateTo):
    data = []
    # query in portions since ES does not allow aggregations with more than 10000 bins
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list)-1):
        data.extend(query4Avg(time_list[i], time_list[i+1]))

    return data


def getStats(df, threshold):
    df = fixMissingMetadata(df)
    # convert to MB
    df['value'] = round(df['value']*1e-6)
    
    # split the data in 3 days
    sitesDf = df.groupby(['src_site', 'dest_site', 'ipv', 'ipv6', pd.Grouper(key='dt', freq='4d')])['value'].mean().to_frame().reset_index()
    
    # get the statistics
    sitesDf['z'] = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: round((x - x.mean())/x.std(),2))
    stdDf = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: x.std()).to_frame().reset_index().rename(columns={'value':'std'})
    stdDf['mean'] = sitesDf.groupby(['src_site','dest_site'])['value'].apply(lambda x: x.mean()).values

    sitesDf = pd.merge(sitesDf, stdDf, left_on=['src_site','dest_site'], right_on=['src_site','dest_site'], how='left')

    # get the % change with respect to the average for the period
    sitesDf['%change'] = round(((sitesDf['value'] - sitesDf['mean'])/sitesDf['mean'])*100)

    # grap the last 3 days period
    last3days = pd.to_datetime(max(sitesDf.dt.unique())).strftime("%Y-%m-%d")

    # return only sites having significant drop in values in the most recent period
    return sitesDf[((sitesDf['z']<=-threshold)|(sitesDf['z']>=threshold))&(sitesDf['dt']==last3days)].rename(columns={'value':'last3days_avg'}).round(2)


def createAlarms(dateFrom, dateTo, alarmsDf, alarmType, minCount=5):
    # we aim for exposing a single site which shows significant change in throughput from/to 5 (default value) other sites in total
    # below we find the total count of unique sites related to a single site name
    src_cnt = alarmsDf[['src_site','ipv', 'ipv6']].value_counts().to_frame().reset_index().rename(columns={0:'cnt', 'src_site': 'site'})
    dest_cnt = alarmsDf[['dest_site','ipv', 'ipv6']].value_counts().to_frame().reset_index().rename(columns={0:'cnt', 'dest_site': 'site'})
    cntDf = pd.concat([src_cnt, dest_cnt]).groupby(['site','ipv', 'ipv6']).sum().reset_index()

    # create the alarm objects
    alarmOnPair = alarms('Networking', 'Sites', alarmType)
    alarmOnMulty = alarms('Networking', 'Sites', f'{alarmType} from/to multiple sites')

    rows2Delete = []



    for site, ipvString, ipv6 in cntDf[cntDf['cnt']>=minCount][['site','ipv', 'ipv6']].values:

        subset = alarmsDf[((alarmsDf['src_site']==site)|(alarmsDf['dest_site']==site))&(alarmsDf['ipv']==ipvString)]
        # build the lists of values
        src_sites, dest_sites, src_change, dest_change = [],[],[],[]

        for idx, row in subset.iterrows():
            if row['src_site'] != site:
                src_sites.append(row['src_site'])
                src_change.append(row['%change'])
            if row['dest_site'] != site:
                dest_sites.append(row['dest_site'])
                dest_change.append(row['%change'])

        all_vals = src_change + dest_change
        above50 = [c for c in all_vals if abs(c)>=50]

        if len(above50)>=minCount:
            # create the alarm source content
            doc = {'from': dateFrom, 'to': dateTo, 'ipv':ipvString, 'ipv6':ipv6,
                   'dest_sites':dest_sites, 'dest_change':dest_change, 
                   'src_sites':src_sites, 'src_change':src_change}
            doc['site'] = site
            
            toHash = ','.join([site, str(ipv6), dateFrom, dateTo])
            doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
            # send the alarm with the proper message
            alarmOnMulty.addAlarm(body=f'{alarmType} from/to multiple sites', tags=[site], source=doc)
            rows2Delete.extend(subset.index.values)

    # delete the rows for which alarms were created
    alarmsDf = alarmsDf.drop(rows2Delete)

    # The rest will be send as 'regular' src-dest alarms
    for doc in alarmsDf[(alarmsDf['%change']<=-50)|(alarmsDf['%change']>=50)][['src_site', 'dest_site', 'ipv', 'ipv6',
                                                                               'last3days_avg', '%change', 'from', 'to']].to_dict('records'):
        toHash = ','.join([doc['src_site'], doc['dest_site'], doc['ipv'], dateFrom, dateTo])
        doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
        alarmOnPair.addAlarm(body=alarmType, tags=[doc['src_site'], doc['dest_site']], source=doc)

now = datetime.utcnow()
dateTo = datetime.strftime(now, '%Y-%m-%d %H:%M')
dateFrom = datetime.strftime(now - timedelta(days=21), '%Y-%m-%d %H:%M')

# get the data
rawDf = pd.DataFrame(queryData(dateFrom, dateTo))
rawDf['dt'] = pd.to_datetime(rawDf['from'], unit='ms')

booleanDictionary = {True: 'ipv6', False: 'ipv4'}
rawDf['ipv'] = rawDf['ipv6'].map(booleanDictionary)

# calculate the statistics
statsDf = getStats(rawDf, 2)
statsDf = getStats(rawDf, 2)
statsDf['from'] = dateFrom
statsDf['to'] = dateTo

# Bandwidth decreased
createAlarms(dateFrom, dateTo, statsDf[(statsDf['z']<=-2)], 'bandwidth decreased')
# Bandwidth recovery
createAlarms(dateFrom, dateTo, statsDf[(statsDf['z']>=2)], 'bandwidth increased')