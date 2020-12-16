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

sign_ploss = pls.df[(pls.df['flag'] == 1)][cols].to_dict(orient='records')
ALARM = alarms('Networking', 'Perfsonar', 'high packet loss')
for item in sign_ploss:
    ALARM.addAlarm(body='', tags=[], source=item)

complete_ploss = pls.df[(pls.df['flag'] == 2)][cols].to_dict(orient='records')
ALARM = alarms('Networking', 'Perfsonar', 'complete packet loss')
for item in complete_ploss:
    ALARM.addAlarm(body='', tags=[], source=item)
