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
import itertools
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


    nullsite = len(tracedf[(tracedf['src_site'].isnull()) | (tracedf['dest_site'].isnull())])

    print(f'{round(((i+j+nullsite)/len(tracedf))*100,0)}% invalid entries removed.')

    tracedf = tracedf[~((tracedf['src_site'].isnull()) | (tracedf['dest_site'].isnull()))]
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
        data = scan(client=hp.es,index='ps_throughput', query=query)

        ret_data = {}
        count = 0
        last_entry = 0
        for item in data:
            ret_data[count] = item['_source']
            dt = item['fields']['timestamp']
            if len(dt) == 1:
                ret_data[count]['timestamp'] = dt[0]
            # print(ret_data[count])
            count+=1


        return ret_data
    except Exception as e:
        print(traceback.format_exc())


def getThroughputData(dt):
    dateFrom, dateTo = dt
    data = queryIndex(dateFrom, dateTo)
    trptdf = pd.DataFrame(data).T

    trptdf = trptdf[~trptdf['throughput'].isnull()]
    trptdf.loc[:, 'throughput_Mb'] = trptdf['throughput'].apply(lambda x: round(x*1e-6))    
    
    trptdf['pair'] = trptdf['src']+'-'+trptdf['dest']
    trptdf.loc[:, 'ssite'] = trptdf['src_site']
    trptdf.loc[:, 'dsite'] = trptdf['dest_site']
    
    trptdf = trptdf[~(trptdf['ssite'].isnull()) & ~(trptdf['dsite'].isnull()) \
    & ~(trptdf['timestamp'].isnull())]
    
    trptdf['site_pair'] = trptdf['ssite']+' -> '+trptdf['dsite']
    
    return trptdf



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
       'destination_reached', 'ipv6', 'asns', 'src_site', 'dest_site',
       'n_hops', 'timestamp', 'src', 'looping',
       'src_host',  'route-sha1', 'ttls', 'rtts', 'dest_host', 'hops', 'n_hops']
    
    fields = [{"field": "timestamp", "format": "strict_date_optional_time"}]

    for args in values:
        dt, src, dest, ipv6, throughput_Mb = args
    
        dtFrom, dtTo = calculateDatetimeRange(dt, "-20"), dt
        query = queryBySrcDest(src, dest, dtFrom, dtTo)
        # print(str(query).replace("\'", "\""))
        
        result =  hp.es.search(index='ps_trace', query=query, sort='timestamp:desc', 
                               fields=fields, size=1, _source=_source)
        for item in result['hits']['hits']:
            # print(item)
            r = item['_source']
            if len(r)>0:
    
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
    rows = trptdf[['timestamp', 'src', 'dest', 'ipv6', 'throughput_Mb']].drop_duplicates().values.tolist()
    value_list = split_list(rows, 500)
    
    with ThreadPoolExecutor(max_workers=100) as pool:
        # print('Starting parallel processing....')
        result = pool.map(queryPSTrace, value_list)
    
    tracedata = []
    for r in result:
        tracedata.extend(r)
    
    tracedf = pd.DataFrame(tracedata)
    
    tracedf = tracedf[~tracedf['hops'].isnull()]
    tracedf.loc[:, 'src_site'] = tracedf['src_site'].str.upper()
    tracedf.loc[:, 'dest_site'] = tracedf['dest_site'].str.upper()
    # tracedf.loc[:, 'dt'] = tracedf['timestamp']
    tracedf['pair'] = tracedf['src']+'-'+tracedf['dest']
    tracedf['ssite'] = tracedf['src_site']
    tracedf['dsite'] = tracedf['dest_site']
    tracedf['site_pair'] = tracedf['ssite']+' -> '+tracedf['dsite']
    tracedf = removeInvalid(tracedf)


    tracedf.loc[:,'hops_str'] = tracedf['hops'].astype(str)
    tracedf.loc[:,'ttls_str'] = tracedf['ttls'].astype(str)
    tracedf.loc[:,'ttls-hops_hash'] = tracedf[['hops_str', 'ttls_str']].apply(hashRows, axis=1)
    
    return tracedf


def pathsAndRoutersData(args):
    router_list, path_list = [],[]
    
    tracedf, values = args

    for row in values:
        data = row.copy()
        data.pop('hops_str')
        data.pop('ttls_str')
        data['traceroute_ts'] = tracedf[(tracedf['src']==row['src']) & (tracedf['dest']==row['dest']) & (tracedf['throughput_ts']==row['throughput_ts'])]['timestamp'].values.tolist()
        hops = [ip.strip(" '") for ip in row['hops_str'].strip("[]").split(',')]
        ttls = [ip.strip(" '") for ip in row['ttls_str'].strip("[]").split(',')]

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
                        router_list.append(router_data)
        
            
        except Exception as e:
            pass

    return router_list, path_list




def split_time_period(start, end, interval_in_days):
    # Convert start and end timestamps to datetime objects
    start_time = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')
    end_time = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Calculate the total time duration in days
    total_duration = (end_time - start_time).days
    
    # Calculate the number of intervals needed
    num_intervals = total_duration // interval_in_days

    time_ranges = []
    current_time = start_time
    
    for _ in range(num_intervals):
        end_of_interval = current_time + timedelta(days=interval_in_days)
        formatted_start = current_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_end = end_of_interval.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        time_ranges.append((formatted_start, formatted_end))
        current_time = end_of_interval
    
    return time_ranges




def get_past_12_hours():
    current_datetime = datetime.now() - timedelta(hours=1)
    past_12_hours = current_datetime - timedelta(hours=12)
    past_12_hours_str = past_12_hours.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    now = current_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return [past_12_hours_str, now]
    


def buildRoutersDataset(dt):
    print(f'======== {dt} ========')
    trptdf = getThroughputData(dt)
    tracedf = getTracerouteData(trptdf)

    # pairs_not_in_trace = len(set(trptdf.pair.unique().tolist()) - set(tracedf.pair.unique().tolist()))
    # num_src_not_in_trace = len(set(trptdf.src.unique().tolist()) - set(tracedf.src.unique().tolist()))
    # num_dest_not_in_trace = len(set(trptdf.dest.unique().tolist()) - set(tracedf.dest.unique().tolist()))


    aggdf = tracedf[['throughput_ts', 'hops_str', 'ttls_str', 'ttls-hops_hash', 'src', 'dest', 'throughput_Mb', 'path_complete', 'destination_reached', 'route-sha1', 'ipv6']].groupby(['src', 'dest', 'ttls-hops_hash', 'hops_str', 'ttls_str', 'throughput_Mb', 'throughput_ts', 'path_complete', 'destination_reached', 'route-sha1', 'ipv6']).agg({'throughput_Mb':['count']})
    aggdf.columns = [col[1] for col in aggdf.columns.values]

    rows = aggdf[aggdf['count'] == 2].reset_index().drop(columns=['count']).to_dict('records')
    value_list = split_list(rows, 200)
    zipped_data = zip(itertools.repeat(tracedf), value_list)

    
    result = []
    with ProcessPoolExecutor(max_workers=20) as pool:
        # print('Starting parallel processing....')
        result = pool.map(pathsAndRoutersData, zipped_data)

    router_list, path_list = [], []
    for r in result:
        router_list.extend(r[0])
        path_list.extend(r[1])

    routerDf = pd.DataFrame(router_list)
    routerDf = routerDf.drop_duplicates(subset=['src', 'dest', 'ttls-hops_hash', 'throughput_Mb', 'throughput_ts', 'router'], keep='first')
    print('Number of router-related documents:', len(routerDf))

    pathDf = pd.DataFrame(path_list)
    # print('Number of path-related documents:', len(pathDf))

    return routerDf.to_dict('records')



def gendata(router_list):
    for i, doc in enumerate(router_list):
        d=  {
            "_index": "routers",
            "_source": doc,
        }

        yield d



def sendToES(router_list):
    for success, info in parallel_bulk(hp.es, gendata(router_list)):
        if not success:
            print('A document failed:', info)



past12h = get_past_12_hours()
router_list = buildRoutersDataset(past12h)
sendToES(router_list)
# router_dataset = pd.DataFrame(router_list)
# router_dataset


# # In case we neet to resend the data for many days
# start, end = ['2023-05-31T00:01:00.000Z', '2023-11-01T00:01:00.000Z']
# interval_in_days = 1  # 1-day interval
# time_ranges = split_time_period(start, end, interval_in_days)
# result = []
# stats_list = []
# with ProcessPoolExecutor(max_workers=30) as pool:
#     # print('Starting parallel processing....')
#     result = pool.map(buildRoutersDataset, time_ranges)

# router_list, path_list = [], []
# for r in result:
#     router_list.extend(r[0])
