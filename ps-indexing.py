# # Checks if Perfsonar data is indexed
#
# Checks number of indexed documents in all perfsonar indices and alerts if any of them is
# significantly less then usual. It sends mails to all the people substribed to that alert.
# It is run every 30 min from a cron job.

import json
import sys
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from datetime import datetime, timedelta
from alarms import alarms
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("agg")

matplotlib.rc('font', **{'size': 12})


with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'port': 9200, 'scheme': 'https'}],
    basic_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

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

    query = {
        "range": {"timestamp": {"gt": ref_start, 'lte': ref_end}}
    }

    res = es.count(index=ind, query=query)
    print(res['count'], 'referent interval query:', query)
    ps_indices[ind][1] = res['count']

    query = {
        "range": {"timestamp": {"gt": ref_end, 'lte': int(sub_end.timestamp() * 1000)}}
    }

    res = es.count(index=ind, query=query)
    print(res['count'], 'referent interval query:', query)
    ps_indices[ind][2] = res['count']


df = pd.DataFrame(ps_indices)
df = df[1:].transpose()
df.columns = ["referent", "current"]
df.referent = df.referent / 2

# df.plot(kind="bar")
# fig = matplotlib.pyplot.gcf()
# fig.set_size_inches(6, 6)
# plt.tight_layout()
# plt.savefig('Images/Check_perfsonar_indexing.png')


df['change'] = df['current'] / df['referent']
df['pr1'] = df['current'] < 10
df['pr2'] = df['change'] < 0.3
df['problem'] = df['pr1'] | df['pr2']
df.head(10)


problematic = df[df['problem'] == True]
print(problematic.head(10))

details = problematic[['referent', 'current']].to_dict()

if problematic.shape[0] > 0:
    ALARM = alarms('Networking', 'Infrastructure', 'indexing')
    ALARM.addAlarm(body='Issue with indexing PS data at UC', tags=['UC'], source=details)
