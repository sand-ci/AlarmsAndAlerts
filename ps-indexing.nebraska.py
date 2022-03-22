# # Checks if Perfsonar data is indexed
#
# Checks number of indexed documents in all perfsonar indices and alerts if any of them is
# significantly less then usual. It sends mails to all the people substribed to that alert.
# It is run every 30 min from a cron job.

import sys
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from alarms import alarms
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("agg")

matplotlib.rc('font', **{'size': 12})


es = Elasticsearch(hosts=[{'host':'gracc.opensciencegrid.org/q/','port':443, 'scheme':'https'}], request_timeout=300)

if es.ping():
    print('connected to ES.')
else:
    print('no connection to ES.')
    sys.exit(1)

# ### define what are the indices to look for
# first number is interval to check (in hours), second is number in 2 previous intervals,
# third is number in current interval.

ps_indices = {
    'ps_meta': [24, 0, 0],
    'ps_owd': [1, 0, 0],
    'ps_packetloss': [1, 0, 0],
    'ps_retransmits': [1, 0, 0],
    'ps_status': [1, 0, 0],
    'ps_throughput': [1, 0, 0],
    'ps_trace': [1, 0, 0]
}

# There is a time offset here - we do now-9 instead of expected now-1.
# two reasons: 1. we get data with a delay
# 2. there is an issue with timezones even data is indexed in UTC.

sub_end = (datetime.utcnow() - timedelta(hours=9)
           ).replace(microsecond=0, second=0, minute=0)
print('end of subject period: ', sub_end)

for ind in ps_indices:
    print("Checking: ", ind)
    tbin = ps_indices[ind][0]

    ref_start = sub_end - timedelta(hours=tbin * 3)
    ref_end = sub_end - timedelta(hours=tbin)
    print('reference interval:', ref_start, ' till ', ref_end)

    ref_start = int(ref_start.timestamp() * 1000)
    ref_end = int(ref_end.timestamp() * 1000)

    types_query = {
        "query": {
            "range": {"timestamp": {"gt": ref_start, 'lte': ref_end}}
        }
    }

    res = es.count(index=ind, body=types_query)
    ps_indices[ind][1] = res['count']

    types_query = {
        "query": {
            "range": {"timestamp": {"gt": ref_end, 'lte': int(sub_end.timestamp() * 1000)}}
        }
    }

    res = es.count(index=ind, body=types_query)
    ps_indices[ind][2] = res['count']


df = pd.DataFrame(ps_indices)
df = df[1:].transpose()
df.columns = ["referent", "current"]
df.referent = df.referent / 2

# df.plot(kind="bar")
# fig = matplotlib.pyplot.gcf()
# fig.set_size_inches(6, 6)
# plt.tight_layout()
# plt.savefig('Images/Check_perfsonar_indexing.Nebraska.png')


df['change'] = df['current'] / df['referent']
df['pr1'] = df['current'] < 10
df['pr2'] = df['change'] < 0.7
df['problem'] = df['pr1'] | df['pr1']
df.head(10)


problematic = df[df['problem'] == True]
print(problematic.head(10))

details = problematic[['referent', 'current']].to_dict()

if problematic.shape[0] > 0:
    ALARM = alarms('Networking', 'Perfsonar', 'indexing')
    ALARM.addAlarm(body='Issue with indexing PS data at Nebraska',
                   tags=['Nebraska'], source=details)


#     S = subscribers()
#     A = alerts.alerts()
#     test_name = 'Alert on Elastic indexing rate [PerfSonar]'
#     users = S.get_immediate_subscribers(test_name)
#     for user in users:
#         body = 'Dear ' + user.name + ',\n\n'
#         body += '\tthis mail is to let you know that there is an issue in indexing
#  Perfsonar data in Nebraska Elasticsearch.\n'
#         A.send_GUN_HTML_mail(
#             'Networking alert',
#             user.email,
#             body,
#             subtitle=test_name,
#             images=[
#                 {
#                     "Title": 'Current vs Referent time',
#                     "Description": "This plot shows number of documents indexed in two intervals. The Current interval is 1h long except for meta data (24h). Referent interval is just before current interval but is twice longer.",
#                     "Filename": "Images/Check_perfsonar_indexing.Nebraska.png",
#                     "Link": "https://gracc.opensciencegrid.org/kibana/goto/36ff22ebb4d87256265eb2b889813191"
#                 }
#             ]
#         )

#         print(user.to_string())
#         A.addAlert(test_name, user.name, 'just an issue.')
