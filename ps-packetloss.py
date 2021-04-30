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

sign_ploss = pls.df[(pls.df['flag'] == 1)][cols].to_dict(orient='records')
ALARM = alarms('Networking', 'Perfsonar', 'high packet loss')
for item in sign_ploss:
    ALARM.addAlarm(body='Link shows high packet loss', tags=[], source=item)


##### Complete packet loss #####

# Hosts/sites showing 100% loss are grouped so that 
# repeated sources/destinations generate a compact message
# under 'Firewall issue' event type

complete_ploss = pls.df[(pls.df['flag'] == 2)]
ALARM = alarms('Networking', 'Perfsonar', 'Firewall issue')

# Get all destination hosts that have more than 5 source hosts 
groups = df.groupby(['dest_host']).agg({'src_host': 'count'}).reset_index()
destWhereCntGrTh5hosts = groups[groups['src_host']>5]['dest_host'].values

if len(destWhereCntGrTh5hosts)>0:
    for host in destWhereCntGrTh5hosts:
        site = complete_ploss[complete_ploss['dest_host']==host]['dest_site'].unique()[0]
        site_list = complete_ploss[complete_ploss['dest_host']==host]['src_site'].values.tolist()
        data = {"site":site, "host":host, "sites":site_list}
        ALARM.addAlarm(body='Firewall issue', tags=site, source=data)



# Get all source hosts that have more than 5 destination hosts 
groups = df.groupby(['src_host']).agg({'dest_host': 'count'}).reset_index()
srcWhereCntGrTh5hosts = groups[groups['dest_host']>5]['src_host'].values

if len(srcWhereCntGrTh5hosts)>0:
    for host in srcWhereCntGrTh5hosts:
        site = complete_ploss[complete_ploss['src_host']==host]['src_site'].unique()[0]
        site_list = complete_ploss[complete_ploss['src_host']==host]['dest_site'].values.tolist()
        data = {"site":site, "host":host, "sites":site_list}
        ALARM.addAlarm(body='Firewall issue', tags=site, source=data)


# Send the reamaining pairs under 'complete packet loss' event type
blocked = complete_ploss[(~complete_ploss['dest_host'].isin(destWhereCntGrTh5hosts)) 
               & (~complete_ploss['src_host'].isin(srcWhereCntGrTh5hosts))][cols].to_dict(orient='records')
ALARM = alarms('Networking', 'Perfsonar', 'complete packet loss')
for item in blocked:
    ALARM.addAlarm(body='Link shows complete packet loss', tags=[item['src_site'], item['dest_site']], source=item)
