# The code looks for the most recent data for each tested node
# It scans all INDICES = ['ps_packetloss', 'ps_owd', 'ps_retransmits', 'ps_throughput', 'ps_trace']
# and extracts the host, ip, site.
# Then looks for additional data in ps_meta.
# Finally, it stores one record for each IP address along with the following fields:
# 'host', 'site', 'administrator', 'email', 'lat', 'lon', 'site_meta', 'site_index', 'last_update'

from elasticsearch.helpers import parallel_bulk
from data_objects.MetaData import MetaData
import utils.helpers as hp

df = MetaData().metaDf

# prepare the data for ES - adding _id and _index to send in bulk
df['_id'] = df['ip']
df['_index'] = "ps_alarms_meta"
df.fillna('', inplace=True)

columns = ['_id', '_index', 'ip', 'host', 'site', 'administrator', 'email', 'lat',
           'lon', 'site_meta', 'site_index', 'last_update']
dataDict = df[columns].to_dict('records')

print(f'Number of hosts: {len(df)}')


def genData(data):
    for d in data:
        yield d


for success, info in parallel_bulk(hp.es, genData(dataDict)):
    if not success:
        print('A document failed:', info)
