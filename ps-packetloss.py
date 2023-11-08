import hashlib
import pandas as pd

from alarms import alarms
import utils.queries as qrs
import utils.helpers as hp
from utils.helpers import timer




def query4Avg(dateFrom, dateTo):
    query = {
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "gt" : dateFrom,
                      "lte": dateTo,
                      "format": "epoch_millis"
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
                            "field" : "src_netsite"
                          }
                        }
                      },
                      {
                        "dest_site" : {
                          "terms" : {
                            "field" : "dest_netsite"
                          }
                        }
                      }
                    ]
                  },
                  "aggs": {
                    "packet_loss": {
                      "avg": {
                        "field": "packet_loss"
                      }
                    }
                  }
                }
              }

    # print(idx, str(query).replace("\'", "\""))
    # print(str(aggregations).replace("\'", "\""))

    aggrs = []

    aggdata = hp.es.search(index='ps_packetloss', query=query, aggregations=aggregations)
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({'pair': str(item['key']['src']+'-'+item['key']['dest']),
                      'from':dateFrom, 'to':dateTo,
                      'ipv6': item['key']['ipv6'],
                      'src': item['key']['src'], 'dest': item['key']['dest'],
                      'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                      'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
                      'value': item['packet_loss']['value'],
                      'doc_count': item['doc_count']
                     })

    return aggrs


@timer
def loadPacketLossData(dateFrom, dateTo):
    data = []
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)

    for i in range(len(time_list)-1):
        data.extend(query4Avg(time_list[i], time_list[i+1]))

    print(f'Period: {dateFrom} - {dateTo}, number of tests: {len(data)}')
    return pd.DataFrame(data)


def getPercentageMeasuresDone(dataDf, dateFrom, dateTo):
    measures_done = dataDf.groupby('pair').agg({'doc_count': 'sum'})

    def findRatio(row, total_minutes):
        if pd.isna(row['doc_count']):
            count = '0'
        else:
            count = str(round((row['doc_count']/total_minutes)*100))+'%'
        return count

    one_test_per_min = hp.CalcMinutes4Period(dateFrom, dateTo)
    measures_done['tests_done'] = measures_done.apply(
        lambda x: findRatio(x, one_test_per_min), axis=1)

    merged = pd.merge(dataDf, measures_done, on='pair', how='left')

    return merged


@timer
def markPairs(dateFrom, dateTo):
    dataDf = loadPacketLossData(dateFrom, dateTo)
    df = getPercentageMeasuresDone(dataDf, dateFrom, dateTo)
    # set value to 0 - we consider there is no issue bellow 2% loss
    # set value to 1 - the pair is marked problematic between 2% and 100% loss
    # set value to 2 - the pair shows 100% loss
    def setFlag(x):
        if x >= 0 and x < 0.02:
            return 0
        elif x >= 0.02 and x < 1:
            return 1
        elif x == 1:
            return 2
        return 'Value is not in range [0,1]'

    df['flag'] = df['value'].apply(lambda val: setFlag(val))
    df.rename(columns={'value': 'avg_value'}, inplace=True)
    df = df.round({'avg_value': 3})

    return df


def findMultiSiteIssues(sign_ploss, threshold=5):
    # get the list of destinations for each source site
    shc = sign_ploss.groupby(['src_host', 'src_site'])['dest_site'].apply(list).reset_index(name='dest_sites').rename(
        columns={'src_host': 'host', 'src_site': 'site'}).to_dict('records')
    # get the list of sources for each destination site
    dhc = sign_ploss.groupby(['dest_host', 'dest_site'])['src_site'].apply(list).reset_index(name='src_sites').rename(
        columns={'dest_host': 'host', 'dest_site': 'site'}).to_dict('records')
    data = {}
    passedThrsh = []

    mergedHosts = shc+dhc

    # count the number of unique sites and add the hosts
    # which report issues with >= 5 other sites to the passedThrsh list
    for items in mergedHosts:
        cnt = 0
        if 'dest_sites' in items:
            cnt = len(set(items['dest_sites']))
        else:
            cnt = len(set(items['src_sites']))

        host = items['host']

        if host not in data.keys():
            data[host] = cnt
        else:
            data[host] = data[host]+cnt
        
        if data[host] >= threshold:
            if host not in passedThrsh:
                passedThrsh.append(host)

    return passedThrsh


##### High packet loss #####

def sendSignificantLossAlarms(plsDf):
    sign_ploss = plsDf[(plsDf['flag'] == 1)][cols]
    sign_ploss['avg_value%'] = round(sign_ploss['avg_value']*100, 1)

    # Create the alarm types
    alarmOnList = alarms('Networking', 'Sites', 'high packet loss on multiple links')
    alarmOnPair = alarms('Networking', 'Sites', 'high packet loss')
    alarmFirewall = alarms('Networking', 'Perfsonar', 'firewall issue')

    multisiteIssues = findMultiSiteIssues(sign_ploss)

    # exclude all on the list having issues with multiple sites and send individual alarm for each pair
    for doc in sign_ploss[~((sign_ploss['dest_host'].isin(multisiteIssues)) |
                            (sign_ploss['src_host'].isin(multisiteIssues)))].to_dict(orient='records'):
        toHash = ','.join([doc['src_site'], doc['dest_site'], dateFrom, dateTo])
        doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
        alarmOnPair.addAlarm(body='Link shows high packet loss', tags=[doc['src_site'], doc['dest_site']], source=doc)

    for host in multisiteIssues:
        src_sites, dest_sites, src_loss, dest_loss = [], [], [], []

        if not sign_ploss[(sign_ploss['src_host'] == host)][['src_site']].empty:
            site = sign_ploss[(sign_ploss['src_host'] == host)][['src_site']].values[0][0]
        else:
            sign_ploss[(sign_ploss['dest_host']==host)][['dest_site']].values[0][0]

        if not sign_ploss[(sign_ploss['src_host']==host)][['dest_site']].empty:
            dest_sites = [l[0] for l in sign_ploss[(sign_ploss['src_host']==host)][['dest_site']].values.tolist()]
            dest_loss = [l[0] for l in sign_ploss[(sign_ploss['src_host']==host)][['avg_value%']].values.tolist()]

        if not sign_ploss[(sign_ploss['dest_host']==host)][['src_site']].empty:
            src_sites = [l[0] for l in sign_ploss[(sign_ploss['dest_host']==host)][['src_site']].values.tolist()]
            src_loss = [l[0] for l in sign_ploss[(sign_ploss['dest_host']==host)][['avg_value%']].values.tolist()]   

        toHash = ','.join([site, host, dateFrom, dateTo])
        if all([1 if l == 100 else 0 for l in src_loss]) and len(dest_sites) == 0:
            doc = {"site": site, "host": host, "sites": src_sites,
                   "alarm_id": hashlib.sha224(toHash.encode('utf-8')).hexdigest(),
                   "from": dateFrom, "to": dateTo}
            alarmFirewall.addAlarm(body='Firewall issue', tags=[site], source=doc)
        else:
            doc = {"site": site, "host": host, "from": dateFrom, "to": dateTo,
                   "alarm_id": hashlib.sha224(toHash.encode('utf-8')).hexdigest(),
                   "dest_sites": dest_sites, "dest_loss%": dest_loss, "src_sites": src_sites, "src_loss%": src_loss}
            alarmOnList.addAlarm(body = f'Site {doc["site"]} shows high packet loss to/from multiple sites',
                                 tags = [site], source = doc)


##### Complete packet loss #####

# Hosts/sites showing 100% loss are grouped so that 
# repeated sources/destinations generate a compact message
# under 'Firewall issue' event type
def sendCompleteLossAlarms(plsDf):
    complete_ploss = plsDf[(plsDf['flag'] == 2)]

    alarmFirewall = alarms('Networking', 'Perfsonar', 'firewall issue')
    alarmCompleteLoss = alarms('Networking', 'Perfsonar', 'complete packet loss')

    # Get the number of sources where the packet loss is 100%
    completeLossDestAgg = complete_ploss.groupby(['dest_host']).agg({'src_host': 'count'}).rename(
        columns={'src_host': 'cnt'}).reset_index()
    # Get the total number of source hosts
    totalNum = plsDf[plsDf['dest_host'].isin(completeLossDestAgg['dest_host'].values)].groupby(['dest_host']).agg(
        {'src_host': 'count'}).rename(columns={'src_host': 'cnt'}).reset_index()

    for dest, cnt in completeLossDestAgg.values:
        if totalNum[totalNum['dest_host']==dest]['cnt'].values[0]==cnt or cnt>=10:
            site = complete_ploss[complete_ploss['dest_host']==dest]['dest_site'].unique()[0]
            site_list = complete_ploss[complete_ploss['dest_host']==dest]['src_site'].values.tolist()
            toHash = ','.join([site, 'complete loss', dateFrom, dateTo])
            doc = {"site": site, "host": dest, "sites": site_list,
                   "alarm_id": hashlib.sha224(toHash.encode('utf-8')).hexdigest(),
                   "from": dateFrom, "to": dateTo}
            alarmFirewall.addAlarm(body='Firewall issue', tags=[site], source=doc)
        else:
            docs = complete_ploss[complete_ploss['dest_host']==dest][cols].to_dict(orient='records')
            for doc in docs:
                toHash = ','.join([doc['src_site'], doc['dest_site'], dateFrom, dateTo])
                doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
                alarmCompleteLoss.addAlarm(body='Link shows complete packet loss',
                                           tags=[doc['src_site'], doc['dest_site']], source=doc)


"""
'src', 'src_host', 'src_site':      info about the source
'dest', 'dest_host', 'dest_site':   info about the destination
'avg_value':                        the average value for the pair
'tests_done':                       % of tests done for the whole period. The calculation is based on the assumption
                                    that there should be 1 measure per minute
"""

dateFrom, dateTo = hp.defaultTimeRange(hours=24)
# dateFrom, dateTo = ['2022-09-26 03:00', '2022-09-27 03:00']
plsDf = markPairs(dateFrom, dateTo)
plsDf = plsDf[plsDf['tests_done']!='0%']
plsDf['from'] = dateFrom
plsDf['to'] = dateTo
plsDf['src_site'] = plsDf['src_site'].str.upper()
plsDf['dest_site'] = plsDf['dest_site'].str.upper()
cols = ['from', 'to', 'src', 'dest', 'src_host', 'dest_host',
        'src_site', 'dest_site', 'avg_value', 'tests_done']

sendSignificantLossAlarms(plsDf)
sendCompleteLossAlarms(plsDf)
