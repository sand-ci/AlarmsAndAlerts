import hashlib
import pandas as pd

from alarms import alarms
import utils.queries as qrs
import utils.helpers as hp
from utils.helpers import timer



@timer
def loadPacketLossData(dateFrom, dateTo):
    data = []
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list)-1):
        data.extend(qrs.query4Avg('ps_packetloss', time_list[i], time_list[i+1]))

    return pd.DataFrame(data)


def getPercentageMeasuresDone(grouped, tempdf):
    measures_done = tempdf.groupby('pair').agg({'doc_count': 'sum'})

    def findRatio(row, total_minutes):
        if pd.isna(row['doc_count']):
            count = '0'
        else:
            count = str(round((row['doc_count']/total_minutes)*100))+'%'
        return count

    one_test_per_min = hp.CalcMinutes4Period(dateFrom, dateTo)
    measures_done['tests_done'] = measures_done.apply(
        lambda x: findRatio(x, one_test_per_min), axis=1)
    grouped = pd.merge(grouped, measures_done, on='pair', how='left')

    return grouped


@timer
def markPairs(dateFrom, dateTo):
    df = pd.DataFrame()

    tempdf = loadPacketLossData(dateFrom, dateTo)
    grouped = tempdf.groupby(['src', 'dest', 'pair', 'src_host', 'dest_host', 'src_site', 'dest_site']).agg(
        {'value': lambda x: x.mean(skipna=False)}, axis=1).reset_index()

    grouped = fixMissingMetadata(grouped)

    # calculate the percentage of measures based on the assumption that ideally measures are done once every minute
    grouped = getPercentageMeasuresDone(grouped, tempdf)

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
        return 'something is wrong'

    grouped['flag'] = grouped['value'].apply(lambda val: setFlag(val))

    df = pd.concat([df, grouped], ignore_index=True)
    df.rename(columns={'value': 'avg_value'}, inplace=True)
    df = df.round({'avg_value': 3})

    return df


@timer
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
        columns={'host': 'host_src', 'site': 'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
        columns={'host': 'host_dest', 'site': 'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['src_site'].fillna(rawDf.pop('site_src'))
    rawDf['dest_site'] = rawDf['dest_site'].fillna(rawDf.pop('site_dest'))
    rawDf['src_host'] = rawDf['src_host'].fillna(rawDf.pop('host_src'))
    rawDf['dest_host'] = rawDf['dest_host'].fillna(rawDf.pop('host_dest'))

    return rawDf


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

        toHash = ','.join([site, dateFrom, dateTo])
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
