import pandas as pd
import re
import traceback
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk, parallel_bulk
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import utils.helpers as hp
import urllib3
urllib3.disable_warnings()
from datetime import datetime, timezone, timedelta
import hashlib
import requests
import ipaddress
import itertools
import time
from functools import partial



def split_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]


def removeInvalid(tracedf):
    pattern = r'^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$'
    tracedf['for_removal'] = 0
    i, j = 0, 0
    for idx, route, pair, hops, ipv, rm in tracedf[tracedf['ipv6']==True][['route-sha1', 'pair', 'hops', 'ipv6', 'for_removal']].itertuples():
        isIPv4 = None
        if len(hops) > 2:
            if ipv == True:
                isIPv4 = re.search(pattern, hops[-1])

                if isIPv4:
                    # print(isIPv4, ipv)
                    tracedf.iat[idx, tracedf.columns.get_loc('for_removal')] = 1
                    i+=1
        else:
            tracedf.iat[idx, tracedf.columns.get_loc('for_removal')] = 1
            j+=1

    nullsite = len(tracedf[(tracedf['src_netsite'].isnull()) | (tracedf['dest_netsite'].isnull())])

    print(f'{round(((i+j+nullsite)/len(tracedf))*100,0)}% invalid entries removed.')

    tracedf = tracedf[~((tracedf['src_netsite'].isnull()) | (tracedf['dest_netsite'].isnull()))]
    tracedf = tracedf[tracedf['for_removal']==0]

    return tracedf.drop(columns=['for_removal'])


def queryIndex(datefrom, dateto):
    query = {
        "fields" : [{"field": "timestamp","format": "strict_date_optional_time"}],
        "query": {
            "bool": {
                    "filter": [
                    {
                        "range": {
                            "timestamp": {
                            "gte": datefrom,
                            "lt": dateto,
                            "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        }
      }
    try:
        # print(str(query).replace("\'", "\""))
        data = scan(client=hp.es,index='ps_throughput', query=query)

        ret_data = {}
        count = 0
        last_entry = 0
        for item in data:
            ret_data[count] = item['_source']
            dt = item['fields']['timestamp']
            if len(dt) == 1:
                ret_data[count]['timestamp'] = dt[0]

            count+=1

        # Check if ret_data is empty
        if not ret_data:
            raise ValueError("No data for the given period.")

        return ret_data
    except Exception as e:
        print(traceback.format_exc())


def getThroughputData(dt):
    try:
        dateFrom, dateTo = dt
        data = queryIndex(dateFrom, dateTo)
        trptdf = pd.DataFrame(data).T

        trptdf = trptdf[~trptdf['throughput'].isnull()]
        trptdf.loc[:, 'throughput_Mb'] = trptdf['throughput'].apply(lambda x: round(x*1e-6))

        trptdf['pair'] = trptdf['src']+'-'+trptdf['dest']
        trptdf.loc[:, 'ssite'] = trptdf['src_netsite']
        trptdf.loc[:, 'dsite'] = trptdf['dest_netsite']

        trptdf = trptdf[~(trptdf['ssite'].isnull()) & ~(trptdf['dsite'].isnull()) \
        & ~(trptdf['timestamp'].isnull())]

        trptdf['site_pair'] = trptdf['ssite']+' -> '+trptdf['dsite']

        return trptdf
    except Exception as e:
        return print(dt, e)


def calculateDatetimeRange(input_datetime_str, delta_str):
    try:
        input_datetime = datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')

        sign = delta_str[0]
        delta_minutes = int(delta_str[1:])

        if sign == '+':
            result_datetime = input_datetime + timedelta(minutes=delta_minutes)
        elif sign == '-':
            result_datetime = input_datetime - timedelta(minutes=delta_minutes)
        else:
            raise ValueError("Invalid delta format. Use '+30' or '-30'.")

        result_datetime_str = result_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        return result_datetime_str
    except ValueError as e:
        return e


def queryBySrcDest(src, dest, dtFrom, dtTo):
    # before 1st Nov, "case_insensitive": True should be used for querying the IPs
    # the reason is that IPv6 were provided mixed - lower and upper case
    # after 1st Nov, the fields' type was changed to IP and parameter is not allowed
    return  {
                "bool": {
                  "must": [
                    {
                      "range": {
                        "timestamp": {
                          "format": "strict_date_optional_time",
                          "gte": dtFrom,
                          "lte": dtTo
                        }
                      }
                    },
                    {
                      "term": {
                        "src": {
                          "value": src
                          # "case_insensitive": True
                        }
                      }
                    },
                    {
                      "term": {
                        "dest": {
                          "value": dest
                          # "case_insensitive": True
                        }
                      }
                    }
                  ],
                  "boost": 1
                }
              }


def queryPSTrace(values):
    # print(args)
    data = []

    _source = ['max_rtt', 'dest', 'src_netsite', 'dest_netsite', 'path_complete',
       'destination_reached', 'ipv6', 'asns', 'src_netsite', 'dest_netsite',
       'n_hops', 'timestamp', 'src', 'looping', 'asns',
       'src_host',  'route-sha1', 'ttls', 'rtts', 'dest_host', 'hops', 'n_hops']

    fields = [{"field": "timestamp", "format": "strict_date_optional_time"}]

    for args in values:
        dt, src, dest, ipv6, throughput_Mb, retransmits = args

        dtFrom, dtTo = calculateDatetimeRange(dt, "-20"), dt
        query = queryBySrcDest(src, dest, dtFrom, dtTo)
        # print(str(query).replace("\'", "\""))

        result =  hp.es.search(index='ps_trace', query=query, sort='timestamp:desc',
                               fields=fields, size=1, _source=_source)
        for item in result['hits']['hits']:
            # print(item)
            r = item['_source']
            if len(r)>0:
                r['retransmits'] = retransmits
                r['throughput_Mb'] = throughput_Mb
                r['throughput_ts'] = dt
                tstamp = item['fields']['timestamp']

                if len(tstamp) == 1:
                    r['timestamp'] = tstamp[0]
                data.append(r)
                break

        dtFrom, dtTo = dt, calculateDatetimeRange(dt, "+20")
        query = queryBySrcDest(src, dest, dtFrom, dtTo)
        result =  hp.es.search(index='ps_trace', query=query, sort='timestamp:asc',
                       fields=fields, size=1, _source=_source)
        for item in result['hits']['hits']:
            r = item['_source']
            # print(item)
            if len(r)>0:
                r['retransmits'] = retransmits
                r['throughput_Mb'] = throughput_Mb
                r['throughput_ts'] = dt
                # r['packet_loss'] = loss
                tstamp = item['fields']['timestamp']

                if len(tstamp) == 1:
                    r['timestamp'] = tstamp[0]
                data.append(r)
                break

    return data


def hashRows(row):
    hops_ttls = row['hops_str']+'-'+row['ttls_str']
    route_hash = hashlib.sha1()
    route_hash.update(";".join(hops_ttls).encode())
    return route_hash.hexdigest()


def getTracerouteData(trptdf):
    result = []
    rows = trptdf[['timestamp', 'src', 'dest', 'ipv6', 'throughput_Mb', 'retransmits']].drop_duplicates().values.tolist()
    value_list = split_list(rows, 500)

    with ThreadPoolExecutor(max_workers=100) as pool:
        # print('Starting parallel processing....')
        result = pool.map(queryPSTrace, value_list)

    tracedata = []
    for r in result:
        tracedata.extend(r)

    tracedf = pd.DataFrame(tracedata)

    tracedf = tracedf[~tracedf['hops'].isnull()]
    tracedf.loc[:, 'src_netsite'] = tracedf['src_netsite'].str.upper()
    tracedf.loc[:, 'dest_netsite'] = tracedf['dest_netsite'].str.upper()
    # tracedf.loc[:, 'dt'] = tracedf['timestamp']
    tracedf['pair'] = tracedf['src']+'-'+tracedf['dest']
    tracedf['ssite'] = tracedf['src_netsite']
    tracedf['dsite'] = tracedf['dest_netsite']
    tracedf['site_pair'] = tracedf['ssite']+' -> '+tracedf['dsite']
    tracedf = removeInvalid(tracedf)


    tracedf.loc[:,'hops_str'] = tracedf['hops'].astype(str)
    tracedf.loc[:,'ttls_str'] = tracedf['ttls'].astype(str)
    tracedf.loc[:,'asns_str'] = tracedf['asns'].astype(str)
    tracedf.loc[:,'rtts_str'] = tracedf['rtts'].astype(str)
    tracedf.loc[:,'ttls-hops_hash'] = tracedf[['hops_str', 'ttls_str']].apply(hashRows, axis=1)

    return tracedf


def pathsAndRoutersData(args):
    router_list, path_list = [],[]

    tracedf, values = args

    for row in values:
        data = row.copy()
        data.pop('hops_str')
        data.pop('ttls_str')
        data.pop('asns_str')
        data.pop('rtts_str')
        # data['traceroute_ts'] = tracedf[(tracedf['src']==row['src']) & (tracedf['dest']==row['dest']) \
        #                         & (tracedf['throughput_ts']==row['throughput_ts'])]['timestamp'].values.tolist()
        hops = [ip.strip(" '") for ip in row['hops_str'].strip("[]").split(',')]
        ttls = [ip.strip(" '") for ip in row['ttls_str'].strip("[]").split(',')]
        asns = [ip.strip(" '") for ip in row['asns_str'].strip("[]").split(',')]
        rtts = [ip.strip(" '") for ip in row['rtts_str'].strip("[]").split(',')]

        try:
            if len(hops)>0:
                path_data = data.copy()
                path_data['first_hop'] = hops[0]+' - '+hops[1]
                path_data['last_hop'] = hops[-2]+' - '+hops[-1]
                path_data['hops'] = hops
                path_list.append(path_data)

                for i, h in enumerate(hops):
                    if h:
                        router_data = data.copy()
                        router_data['router'] = h
                        router_data['ttl'] = ttls[i]
                        router_data['asn'] = asns[i]
                        router_data['rtt'] = rtts[i]
                        router_list.append(router_data)


        except Exception as e:
            pass

    return router_list, path_list


def split_time_period(start_str, end_str, bin_hours=12):
    # Parse the start and end times
    start_time = datetime.strptime(start_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    end_time = datetime.strptime(end_str, '%Y-%m-%dT%H:%M:%S.%fZ')

    # Calculate the bin duration as a timedelta object
    bin_duration = timedelta(hours=bin_hours)

    # Initialize the list of bins
    bins = []

    # Initialize the current start time for binning
    current_start_time = start_time

    # Loop to create bins
    while current_start_time < end_time:
        # Calculate the current end time for the bin
        current_end_time = current_start_time + bin_duration

        # Ensure the current end time does not exceed the overall end time
        if current_end_time > end_time:
            current_end_time = end_time

        # Append the current bin as [start, end] in the specified format
        bins.append([
            current_start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            current_end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        ])

        # Update the start time for the next bin
        current_start_time = current_end_time

    return bins


def get_past_12_hours():
    current_datetime = datetime.now() - timedelta(hours=1)
    past_12_hours = current_datetime - timedelta(hours=12)
    past_12_hours_str = past_12_hours.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    now = current_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return [past_12_hours_str, now]


def getMeta():
    meta = []
    data = scan(hp.es, index='ps_alarms_meta')
    for item in data:
        meta.append(item['_source'])

    if meta:
        mdf = pd.DataFrame(meta)

    return mdf


class IPDefinedError(Exception):
    pass


def get_as_number(ip):
    try:
        # Check if the IP is private
        if ipaddress.ip_address(ip).is_private:
            raise IPDefinedError(f"IPv4 address {ip} is already defined as Private-Use Networks via RFC 1918.")

        url = f"http://ip-api.com/json/{ip}"
        response = requests.get(url)
        data = response.json()

        # Check if the request was successful
        if response.status_code != 200 or data['status'] != 'success':
            # print(response)
            return -2


        as_info = data.get('as', '')
        as_number = re.search(r'AS(\d+)', as_info)
        return int(as_number.group(1)) if as_number else 0

    except IPDefinedError as e:
        # print(e)
        return -1

    except Exception as e:
        # print(f"An error occurred: {e}", r)
        return 0


def add_missing_ips(ips, ttls):
    max_ttl = max(max(ttls[0]), max(ttls[1]))  # Find the maximum TTL value
    filled_ips = [[None] * (max_ttl + 1) for _ in range(2)]  # Initialize filled IP lists
    added_ips = [dict(), dict()]  # Keep track of IPs added to each list as TTL-IP pairs

    # Fill in the IPs based on their TTLs
    for i, ttl_list in enumerate(ttls):
        for ttl, ip in zip(ttl_list, ips[i]):
            filled_ips[i][ttl] = ip

    # Fill missing IPs from one list to the other and track the changes
    for i in range(2):
        other_i = (i + 1) % 2
        for ttl, ip in enumerate(filled_ips[other_i]):
            if ip is not None and filled_ips[i][ttl] is None:
                filled_ips[i][ttl] = ip
                added_ips[i][ttl] = ip  # Record the added IP and its TTL

    # Remove None values from filled IP lists
    filled_ips = [[ip for ip in ip_list if ip is not None] for ip_list in filled_ips]

    return filled_ips, added_ips


def segment_similarity(seg1, seg2):
    matches = sum(c1 == c2 for c1, c2 in zip(seg1, seg2))

    return matches / max(len(seg1), len(seg2))


def ip_similarity(ip1, ip2):
    ip1_obj = ipaddress.ip_address(ip1)
    ip2_obj = ipaddress.ip_address(ip2)

    if ip1_obj.version != ip2_obj.version:
        return 0

    segments1 = ip1_obj.exploded.split(':') if ip1_obj.version == 6 else ip1_obj.exploded.split('.')
    segments2 = ip2_obj.exploded.split(':') if ip2_obj.version == 6 else ip2_obj.exploded.split('.')

    similarity = sum(segment_similarity(seg1, seg2) for seg1, seg2 in zip(segments1, segments2))

    return similarity / len(segments1)


def find_start_of_overlap(list1, list2):
    # Find the index in list1 where the overlap with list2 starts
    for i in range(len(list1)):
        if list1[i:] == list2[:len(list1) - i]:
            return i
    return 0  # Default to starting at index 0 if no overlap found


def calculate_similarity(list1, list2, ttl1=None, ttl2=None):
    added_ips = []
    if ttl1 is not None and ttl2 is not None:
        (list1, list2), added_ips = add_missing_ips([list1, list2], [ttl1, ttl2])

    start_index = find_start_of_overlap(list1, list2)
    overlap_length = min(len(list1) - start_index, len(list2))

    if set(list2).issubset(set(list1)) or set(list1).issubset(set(list2)):
        # Find the number of additional IPs
        extra_ips = abs(len(list1) - len(list2))
        # Calculate the similarity score
        similarity_score = 1 - 0.1 * extra_ips

    else: 
        total_similarity = sum(ip_similarity(ip1, ip2) for ip1, ip2 in zip(list1[start_index:start_index + overlap_length], list2[:overlap_length]))
        extra_ips_penalty = 0.1 * (abs(len(list1) - len(list2)) + start_index)
        similarity_score = (total_similarity - extra_ips_penalty) / overlap_length

    return round(similarity_score,3), added_ips


def get_similarity_scores(routerDf):
    aggdf = routerDf.groupby(['throughput_ts', 'src', 'dest'])[['ttls-hops_hash']].nunique()
    aggdf['similarity_score'] = 1
    aggdf['added_ips'] = ''
    for i, items in enumerate(aggdf[aggdf['ttls-hops_hash']>1].index.tolist()):
        routes = routerDf[(routerDf['throughput_ts']==items[0]) & (routerDf['src']==items[1]) & (routerDf['dest']==items[2])]\
                            .groupby(['ttls-hops_hash'])['router'].apply(list).values
        ttl = routerDf[(routerDf['throughput_ts']==items[0]) & (routerDf['src']==items[1]) & (routerDf['dest']==items[2])]\
                            .groupby(['ttls-hops_hash'])['ttl'].apply(list).values
        asns = routerDf[(routerDf['throughput_ts']==items[0]) & (routerDf['src']==items[1]) & (routerDf['dest']==items[2])]\
                            .groupby(['ttls-hops_hash'])['asn'].apply(list).values

        value = routerDf[(routerDf['throughput_ts']==items[0]) & (routerDf['src']==items[1]) & (routerDf['dest']==items[2])]['throughput_Mb'].mean()

        if len(ttl.tolist()[0]) != len(asns.tolist()[0]):
            print('This should not happen!')

        r1,r2 = routes.tolist()
        ttl1,ttl2 = ttl.tolist()
        similarity, added_ips = calculate_similarity(r1,r2,ttl1,ttl2)
        aggdf.at[items, 'similarity_score'] = similarity
        aggdf.at[items, 'added_ips'] = added_ips


    aggdf.rename(columns={'ttls-hops_hash':'stable'}, inplace=True)
    aggdf['stable'] = aggdf['stable'].replace({1: True, 2: False})
    routerDf = pd.merge(routerDf, aggdf[['similarity_score', 'stable']], on=['throughput_ts', 'src', 'dest'])

    return routerDf


def replace_zero_asn(router_to_asn, row):
    if row['asn'] == 0:
        return router_to_asn.get(row['router'], 0)  # Get the asn from the mapping, default to 0 if not found
    else:
        return row['asn']


def try_recover_ASNs(routerDf):
    asn_mode = (
        routerDf[routerDf['asn'] != 0]  # Filter out rows where 'asn' is 0
        .groupby('router')['asn']  # Group by 'router'
        .agg(lambda x: x.mode()[0])  # Find the mode (most common value) for 'asn'
        .reset_index()  # Reset index to turn the result into a DataFrame
    )

    # Create a dictionary for mapping routers to most common asn
    router_to_asn = dict(zip(asn_mode['router'], asn_mode['asn']))
    routerDf['asn'] = routerDf.apply(lambda row: replace_zero_asn(router_to_asn, row), axis=1)

    return routerDf


def buildRoutersDataset(dt):
    trptdf = getThroughputData(dt)
    tracedf = getTracerouteData(trptdf)

    df = tracedf[['timestamp','throughput_ts', 'hops_str', 'ttls_str', 'asns_str', 'rtts_str', 'ttls-hops_hash',
                 'src', 'dest', 'throughput_Mb', 'retransmits', 'path_complete', 'destination_reached',
                 'route-sha1', 'ipv6', 'src_host', 'dest_host']].copy()

    df.rename(columns={'timestamp': 'traceroute_ts'}, inplace=True)

    rows = df.to_dict('records')
    value_list = split_list(rows, 200)
    zipped_data = zip(itertools.repeat(tracedf), value_list)

    result = []
    with ProcessPoolExecutor(max_workers=5) as pool:
        # print('Starting parallel processing....')
        result = pool.map(pathsAndRoutersData, zipped_data)

    router_list, path_list = [], []
    for r in result:
        router_list.extend(r[0])
        path_list.extend(r[1])


    routerDf = pd.DataFrame(router_list)

    routerDf.loc[:, 'src'] = routerDf['src'].str.upper()
    routerDf.loc[:, 'dest'] = routerDf['dest'].str.upper()
    routerDf.loc[:, 'router'] = routerDf['router'].str.upper()
    routerDf.loc[:,'ttl'] = routerDf['ttl'].astype(int)
    routerDf.loc[:,'rtt'] = routerDf['rtt'].astype(float)
    routerDf.loc[:,'asn'] = routerDf['asn'].astype(int)

    mdf = getMeta()
    mdf = mdf.drop_duplicates(subset=['ip', 'site'], keep='first')
    routerDf = pd.merge(routerDf, mdf[['ip', 'site']], left_on='src', right_on='ip', how='left')
    routerDf = pd.merge(routerDf, mdf[['ip', 'site']], left_on='dest', right_on='ip', how='left', suffixes=('_src', '_dest'))

    routerDf = routerDf.drop(columns=['ip_src', 'ip_dest']).rename(columns={'site_src': 'src_site', 'site_dest': 'dest_site'})
    routerDf['src_site'].fillna('', inplace=True)
    routerDf['dest_site'].fillna('', inplace=True)

    routerDf = routerDf.drop_duplicates(subset=['traceroute_ts', 'throughput_ts', 'ttls-hops_hash', 'src', 'dest',
           'throughput_Mb', 'retransmits', 'path_complete', 'destination_reached', 'route-sha1',
           'ipv6', 'src_host', 'dest_host', 'router', 'ttl', 'asn', 'rtt'], keep='first')

    routerDf = try_recover_ASNs(routerDf)
    routerDf = get_similarity_scores(routerDf)

    print('Number of router-related documents:', len(routerDf))
    router_list = routerDf.to_dict('records')

    return router_list


def sendToES(router_list, max_retries=3, retry_delay=5):
    batch = []
    batch_size = 500
    batches = [router_list[i:i+batch_size] for i in range(0, len(router_list), batch_size)]

    for batch in batches:
        attempts = 0
        while attempts < max_retries:
            try:
                bulk(hp.es, batch, index='routers')
                break  # Exit the retry loop if successful
            except Exception as e:
                attempts += 1
                print(f"Attempt {attempts} failed for batch {len(batch)}.\n")
                print(f"Error: {e}")

                if attempts < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)  # Wait before retrying
                else:
                    print("Max retries reached. Moving to the next batch.")
                    print(batch[0], batch[-1])
                    if hasattr(e, 'errors'):
                        for error_item in e.errors:
                            print(error_item)
                    break

    print("All data sent successfully!")


past12h = get_past_12_hours()
# past12h = ['2024-01-19T12:05:00.000Z', '2024-01-21T05:34:14.122910Z']
router_list = buildRoutersDataset(past12h)
sendToES(router_list)


# # # In case we neet to resend the data for many days
# # %%time
# def updateMapping(router_list, router_to_asn):
#     routerDf = pd.DataFrame(router_list)
#     asn_mode = (
#             routerDf[routerDf['asn'] != 0]  # Filter out rows where 'asn' is 0
#             .groupby('router')['asn']  # Group by 'router'
#             .agg(lambda x: x.mode()[0])  # Find the mode (most common value) for 'asn'
#             .reset_index()  # Reset index to turn the result into a DataFrame
#         )

#         # Create a dictionary for mapping routers to most common asn
#     additional_router_to_asn = dict(zip(asn_mode['router'], asn_mode['asn']))
#     router_to_asn.update(additional_router_to_asn)
#     routerDf['asn'] = routerDf.apply(lambda row: replace_zero_asn(router_to_asn, row), axis=1)

#     router_list = routerDf.to_dict('records')
#     return router_to_asn, router_list


# routerDf = pd.DataFrame(df)
# # router_to_asn = {k:v for k, v in router_to_asn.items() if v not in [0,-1]}
# router_to_asn = {}
# asn_mode = (
#         routerDf[(routerDf['asn'] != 0) ]  # Filter out rows where 'asn' is 0
#         .groupby('router')['asn']  # Group by 'router'
#         .agg(lambda x: x.mode()[0])  # Find the mode (most common value) for 'asn'
#         .reset_index()  # Reset index to turn the result into a DataFrame
#     )
#     # Create a dictionary for mapping routers to most common asn
# additional_router_to_asn = dict(zip(asn_mode['router'], asn_mode['asn']))
# router_to_asn.update(additional_router_to_asn)


# weeks = split_time_period('2023-01-01T00:05:00.000Z', '2024-01-19T12:05:00.000Z', 168)

# for one_weeek in weeks:
#     start, end = one_weeek
#     time_ranges = split_time_period(start, end, 12)
#     result = []
#     stats_list = []
    
#     with ProcessPoolExecutor(max_workers=10) as pool:
#         # print('Starting parallel processing....')
#         result = pool.map(buildRoutersDataset, time_ranges)
#         for router_list in result:
#             router_to_asn, router_list = updateMapping(router_list, router_to_asn)
#             sendToES(router_list)

#     print(f'---- done for period {one_weeek} -----')
