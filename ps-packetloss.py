from alarms import alarms
import pandas as pd

from data_objects.DataLoader import PrtoblematicPairsDataLoader
pls = PrtoblematicPairsDataLoader('ps_packetloss')


"""
'src', 'src_host', 'src_site':      info about the source
'dest', 'dest_host', 'dest_site':   info about the destination
'avg_value':                        the average value for the pair
'tests_done':                       % of tests done for the whole period. The calculation is based on the assumption
                                    that there should be 1 measure per minute
"""

cols = ['src', 'dest', 'src_host', 'dest_host',
        'src_site', 'dest_site', 'avg_value', 'tests_done']


##### High packet loss #####

sign_ploss = pls.df[(pls.df['flag'] == 1)][cols]
# convert to %
sign_ploss['avg_value'] = sign_ploss['avg_value'].apply(lambda x: str(round((x)*100))+'%')


# Loop over all src/dest_hosts and grab the sites they show problems with
data = {}
for host in list(set(sign_ploss['src_host'].to_list())):
    site = sign_ploss[(sign_ploss['src_host']==host)]['src_site'].values[0]
    dest_sites = sign_ploss[sign_ploss['src_host']==host]['dest_site'].values.tolist()
    data[host] = {"site":site, "host":host, "dest_sites":dest_sites, "src_sites":[]}

for host in list(set(sign_ploss['dest_host'].to_list())):
    site = sign_ploss[(sign_ploss['dest_host']==host)]['dest_site'].values[0]
    src_sites = sign_ploss[sign_ploss['dest_host']==host]['src_site'].values.tolist()
    if host in data.keys():
        data[host]["src_sites"] = src_sites
    else: 
        data[host] = {"site":site, "host":host, "dest_sites":[], "src_sites":src_sites}

# Create the alarm types
alarmOnList = alarms('Networking', 'Perfsonar', 'high packet loss on multiple links')
alarmOnPair = alarms('Networking', 'Perfsonar', 'high packet loss')

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

complete_ploss = pls.df[(pls.df['flag'] == 2)]
alarmFirewall = alarms('Networking', 'Perfsonar', 'Firewall issue')

# Get all destination hosts that have more than 5 source hosts 
groups = complete_ploss.groupby(['dest_host']).agg({'src_host': 'count'}).reset_index()
destWhereCntGrTh5hosts = groups[groups['src_host']>5]['dest_host'].values

if len(destWhereCntGrTh5hosts)>0:
    for host in destWhereCntGrTh5hosts:
        site = complete_ploss[complete_ploss['dest_host']==host]['dest_site'].unique()[0]
        site_list = complete_ploss[complete_ploss['dest_host']==host]['src_site'].values.tolist()
        data = {"site":site, "host":host, "sites":site_list}
        alarmFirewall.addAlarm(body='Firewall issue', tags=[site], source=data)


# Send the reamaining pairs under 'complete packet loss' event type
blocked = complete_ploss[(~complete_ploss['dest_host'].isin(destWhereCntGrTh5hosts))][cols].to_dict(orient='records')
alarmCompleteLoss = alarms('Networking', 'Perfsonar', 'complete packet loss')
for item in blocked:
    alarmCompleteLoss.addAlarm(body='Link shows complete packet loss', tags=[item['src_site'], item['dest_site']], source=item)
