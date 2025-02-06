from elasticsearch import Elasticsearch, helpers
# from collections import Counter
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
# import time
# import matplotlib.pyplot as plt
import json
from alarms import alarms

# ### loading the data (owd, src/dest pair of nodes) in the last 24 h

tend = datetime.now()
tstart = tend - timedelta(days=1)
start = pd.Timestamp(tstart)
end = pd.Timestamp(tend)
dateFrom = datetime.strftime(start, '%Y-%m-%d %H:%M')
dateTo = datetime.strftime(end, '%Y-%m-%d %H:%M')

with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'port':9200, 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    request_timeout=60)

es.ping()


my_query = {
    '_source': ['delay_mean', 'dest_host', 'src_host', 'src_netsite', 'dest_netsite'],
    'query': {
        'bool': {
            'must': [{
                'range': {
                    'timestamp': {
                        'gte': start.strftime('%Y%m%dT%H%M00Z'),
                        'lt': end.strftime('%Y%m%dT%H%M00Z')}
                }
            }
            ]
        }
    }
}

res = helpers.scan(client=es, index="ps_owd", query=my_query)

count = 0  # tests
delay_mean = []
dest_host = []
src_host = []
src_site, dest_site = [], []

for r in res:
    delay_mean.append(r.get('_source').get('delay_mean'))
    dest_host.append(r.get('_source').get('dest_host'))
    src_host.append(r.get('_source').get('src_host'))
    src_site.append(r.get('_source').get('src_netsite'))
    dest_site.append(r.get('_source').get('dest_netsite'))
    count += 1
    if not count % 100000:
        print(count)
df = pd.DataFrame({'delay_mean': delay_mean, 'src_host': src_host, 'dest_host': dest_host,
                   'src_site': src_site, 'dest_site': dest_site})

# prepare to tag sites as well
ddf = df[['src_host', 'dest_host', 'src_site', 'dest_site']].drop_duplicates()

# ### plotting the histogram, providing some basic stats

# fig, ax = plt.subplots()
# delay_hist = plt.hist(df.delay_mean, bins=60, range=(
#     min(df['delay_mean']), max(df['delay_mean'])))
# ax.set_yscale('log')
# print(df.shape)
# print(df.describe())
# mean = df.delay_mean.mean()
# variance_orig = df.loc[:, 'delay_mean'].var()
# print(variance_orig)
# print('minimum delay_mean is: ', min(
#     df['delay_mean']), 'maximum delay_mean is', max(df['delay_mean']))


# ### filtering out the hosts with too high OWDs

bad_hosts_df = df[(df['delay_mean'] <= -100000) | (df['delay_mean'] >= 100000)]
list_of_hosts_with_bad_measurements = []

while not bad_hosts_df.empty:
    # get list of hosts with most bad measurements
    sh = bad_hosts_df.src_host.value_counts()
    dh = bad_hosts_df.dest_host.value_counts()
    # sum = lambda sh, dh: np.nansum(sh + dh)
    sum = sh.add(dh, fill_value=0).sort_values(ascending=False)
    host_to_remove = sum.index[0]
#    print(host_to_remove)
    list_of_hosts_with_bad_measurements.append(host_to_remove)
    bad_hosts_df = bad_hosts_df[(bad_hosts_df['src_host'] == host_to_remove)]
    bad_hosts_df = bad_hosts_df[(bad_hosts_df['dest_host'] == host_to_remove)]
#    print(bad_hosts_df)

# print("List of hosts with bad measurements:",
#       list_of_hosts_with_bad_measurements)

if len(list_of_hosts_with_bad_measurements):
    ALARM = alarms('Networking', 'Infrastructure', 'bad owd measurements')
    for bh in list_of_hosts_with_bad_measurements:
        # add site names to the list of tags
        site = ''
        if not ddf[ddf['src_host'] == bh].empty:
            site = ddf[ddf['src_host'] == bh]['src_site'].values[0]
        else:
            site = ddf[ddf['dest_host'] == bh]['dest_site'].values[0]
        tags = [bh, site.upper()] if site is not None else [bh]

        ALARM.addAlarm(body=bh, tags=tags)

# ### removing hosts with too high measurements

for node in list_of_hosts_with_bad_measurements:
    df = df[(df.src_host != node) & (df.dest_host != node)]
# print('remaining rows:', df.shape[0])


# ### removing one sided nodes and getting the final dataframe to work with

all_nodes = np.unique(df[['src_host', 'dest_host']].values)
sc_nodes = np.unique(df['src_host'].values)
ds_nodes = np.unique(df['dest_host'].values)

one_sided_nodes = list(set(sc_nodes).symmetric_difference(ds_nodes))

# print('one sided nodes: ', one_sided_nodes)

# removes one sided nodes from all nodes
correctable_nodes = np.setdiff1d(all_nodes, one_sided_nodes)

# print('one sided nodes', len(one_sided_nodes))
# print('correctable nodes ', len(correctable_nodes))

for node in one_sided_nodes:
    df = df[(df.src_host != node) & (df.dest_host != node)]
# print('remaining rows:', df.shape[0])

# print('minimum delay_mean is: ', min(
#     df['delay_mean']), 'maximum delay_mean is', max(df['delay_mean']))
# print(df['delay_mean'].var())


# ### creating a new dataframe with the corrections node-wise

dfc = df.copy()
host_dict = {}

review = {'node': [], 'measurements': [],
          'owd as source': [], 'owd as destination': []}
print('current variance:{:.2f}'.format(dfc.delay_mean.var()))
for node in correctable_nodes:
    df_tmp = dfc[(dfc.src_host == node) | (dfc.dest_host == node)]
    review["owd as source"].append(
        df_tmp[df_tmp.src_host == node].delay_mean.mean())
    review["owd as destination"].append(
        df_tmp[df_tmp.dest_host == node].delay_mean.mean())
    review["node"].append(node)
    review["measurements"].append(df_tmp.shape[0])
df_rev = pd.DataFrame.from_dict(review)
df_rev['correction'] = (df_rev['owd as source']-df_rev['owd as destination'])/2
df_hosts = df_rev.drop(
    ['measurements', 'owd as source', 'owd as destination'], axis=1)
print(df_hosts)

df_hosts.correction.isna().sum()


# Creating alarms when correction required is larger than 100 ms.

df_corr = df_hosts[abs(df_hosts['correction']) > 100]

ALARM = alarms('Networking', 'Infrastructure', 'large clock correction')
for (node, correction) in df_corr.values:
    # add site names to the list of tags
    site = ''
    if not ddf[ddf['src_host'] == node].empty:
        site = ddf[ddf['src_host'] == node]['src_site'].values[0]
    else:
        site = ddf[ddf['dest_host'] == node]['dest_site'].values[0]
    tags = [node, site.upper()] if site is not None else [node]

    ALARM.addAlarm(
         body=node+" "+str(correction),
         tags=tags,
         source={"node": node, "correction": correction, "from": dateFrom, "to": dateTo}
    )

# print(df_hosts.shape, max(df_hosts.correction), min(df_hosts.correction))
# plt.hist(df_hosts.correction, range=(
#     min(df_hosts['correction']), max(df_hosts['correction'])))

df = df.assign(dmc=df.delay_mean)

for (node, correction) in df_hosts.values:
    df.loc[(df['src_host'] == node), 'dmc'] = df['dmc']-correction
    df.loc[(df['dest_host'] == node), 'dmc'] = df['dmc']+correction
# print(df)
# print('variance after', df.dmc.var())
# print('variance before', df['delay_mean'].var())
# print('minimum delay_mean is: ', min(
#     df['delay_mean']), 'maximum delay_mean is', max(df['delay_mean']))
# print('minimum delay_mean_corrected is: ', min(
#     df['dmc']), 'maximum delay_mean_corrested is', max(df['dmc']))

# fig, ax = plt.subplots()
# delay_hist_corr = plt.hist(
#     df.dmc, bins=60, range=(min(df['dmc']), max(df['dmc'])))
# ax.set_yscale('log')
# print(df.shape)
# df.dmc.describe()
# variance_corr = df.loc[:, 'dmc'].var()
# print(variance_corr)

# bins = np.linspace(min(df['delay_mean']), max(df['delay_mean']))
# fig, ax = plt.subplots(figsize=(10, 10))
# ax.set_yscale('log')
# plt.hist(df.delay_mean, bins=bins, histtype='step', label='messed up')
# plt.hist(df.dmc, bins=bins, histtype='step', label='corrected')
# plt.legend(loc='upper right')
# plt.show()

# removing one-sided nodes after the corrections are applied
for node in one_sided_nodes:
    df = df[(df.src_host != node) & (df.dest_host != node)]
# print('remaining rows:', df.shape[0])

# bins = np.linspace(min(df['delay_mean']), max(df['delay_mean']))
# fig, ax = plt.subplots(figsize=(10, 10))
# ax.set_yscale('log')
# plt.hist(df.delay_mean, bins=bins, histtype='step', label='messed up')
# plt.hist(df.dmc, bins=bins, histtype='step', label='corrected')
# plt.legend(loc='upper right')
# plt.show()
# print(min(df['delay_mean']), max(df['delay_mean']))
# print(min(df['dmc']), max(df['dmc']))
