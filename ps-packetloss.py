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

    df = df.append(grouped, ignore_index=True)
    df.rename(columns={'value': 'avg_value'}, inplace=True)
    df = df.round({'avg_value': 3})

    return df


@timer
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
                columns={'host':'host_src','site':'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
                columns={'host':'host_dest','site':'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['src_site'].fillna(rawDf.pop('site_src'))
    rawDf['dest_site'] = rawDf['dest_site'].fillna(rawDf.pop('site_dest'))
    rawDf['src_host'] = rawDf['src_host'].fillna(rawDf.pop('host_src'))
    rawDf['dest_host'] = rawDf['dest_host'].fillna(rawDf.pop('host_dest'))

    return rawDf


"""
'src', 'src_host', 'src_site':      info about the source
'dest', 'dest_host', 'dest_site':   info about the destination
'avg_value':                        the average value for the pair
'tests_done':                       % of tests done for the whole period. The calculation is based on the assumption
                                    that there should be 1 measure per minute
"""

dateFrom, dateTo = hp.defaultTimeRange(hours=24)
plsDf = markPairs(dateFrom, dateTo)
cols = ['src', 'dest', 'src_host', 'dest_host',
        'src_site', 'dest_site', 'avg_value', 'tests_done']


##### High packet loss #####

sign_ploss = plsDf[(plsDf['flag'] == 1)][cols]
sign_ploss['avg_value%'] = round(sign_ploss['avg_value']*100,1)

# Loop over all src/dest_hosts and grab the sites they show problems with
data = {}
for host in list(set(sign_ploss['src_host'].to_list())):
    site = sign_ploss[(sign_ploss['src_host']==host)]['src_site'].values[0]
    dest_sites = sign_ploss[sign_ploss['src_host']==host]['dest_site'].values.tolist()
    loss = sign_ploss[sign_ploss['src_host']==host]['avg_value%'].values.tolist()
    data[host] = {"site":site, "host":host, "dest_sites":dest_sites, "dest_loss":loss, "src_sites":[], "src_loss":[]}

for host in list(set(sign_ploss['dest_host'].to_list())):
    site = sign_ploss[(sign_ploss['dest_host']==host)]['dest_site'].values[0]
    src_sites = sign_ploss[sign_ploss['dest_host']==host]['src_site'].values.tolist()
    loss = sign_ploss[sign_ploss['dest_host']==host]['avg_value%'].values.tolist()

    if host in data.keys():
        data[host]["src_sites"] = src_sites
        data[host]["src_loss"] = loss
    else: 
        data[host] = {"site":site, "host":host, "dest_sites":[], "dest_loss":[], "src_sites":src_sites, "src_loss":loss}


# Create the alarm types
alarmOnList = alarms('Networking', 'Sites', 'high packet loss on multiple links')
alarmOnPair = alarms('Networking', 'Sites', 'high packet loss')

# Loop over all hosts and generate alarms depending on the number of sites it shows problems with
for key, item in data.items():
    cnt = len(item['src_sites']) + len(item['dest_sites'])

    # If the host shows problems in one direction or both to more than 5 other sites, send an alarm with lists of sites
    if cnt>=5:
        alarmOnList.addAlarm(body=f'Site {item["site"]} shows high packet loss to/from multiple sites', tags=[item['site']], source=item)
    else:
    # Otherwise send a standard alarm including more fields
        for src in data[key]['src_sites']:
            doc = sign_ploss[(sign_ploss['dest_host']==key) & (sign_ploss['src_site']==src)][cols].to_dict(orient='records')[0]
            alarmOnPair.addAlarm(body='Link shows high packet loss', tags=[doc['src_site'], doc['dest_site']], source=doc)
        for dest in data[key]['dest_sites']:
            doc = sign_ploss[(sign_ploss['src_host']==key) & (sign_ploss['dest_site']==dest)][cols].to_dict(orient='records')[0]
            alarmOnPair.addAlarm(body='Link shows high packet loss', tags=[doc['src_site'], doc['dest_site']], source=doc)



##### Complete packet loss #####

# Hosts/sites showing 100% loss are grouped so that 
# repeated sources/destinations generate a compact message
# under 'Firewall issue' event type

complete_ploss = plsDf[(plsDf['flag'] == 2)]
alarmFirewall = alarms('Networking', 'Perfsonar', 'firewall issue')


# Get all destination hosts that have more than 5 source hosts 
groups = complete_ploss.groupby(['dest_host']).agg({'src_host': 'count'}).reset_index()
destWhereCntGrTh10hosts = groups[groups['src_host']>10]['dest_host'].values

if len(destWhereCntGrTh10hosts)>0:
    for host in destWhereCntGrTh10hosts:
        site = complete_ploss[complete_ploss['dest_host']==host]['dest_site'].unique()[0]
        site_list = complete_ploss[complete_ploss['dest_host']==host]['src_site'].values.tolist()
        data = {"site":site, "host":host, "sites":site_list}
        alarmFirewall.addAlarm(body='Firewall issue', tags=[site], source=data)


# Send the reamaining pairs under 'complete packet loss' event type
blocked = complete_ploss[(~complete_ploss['dest_host'].isin(destWhereCntGrTh10hosts))][cols].to_dict(orient='records')
alarmCompleteLoss = alarms('Networking', 'Perfsonar', 'complete packet loss')
for item in blocked:
    alarmCompleteLoss.addAlarm(body='Link shows complete packet loss', tags=[item['src_site'], item['dest_site']], source=item)
