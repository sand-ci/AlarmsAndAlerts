# ps-hosts-not-found - This module checks whether expected hosts are found in the Elasticsearch throughput/latency/trace
#                    data the day before yesterday and compares it with
#                    the expected hosts from the provided mesh configuration.
#                    It queries Elasticsearch for specific test data (throughput/latency/trace)
#                    within a 24-hour time range and verifies if the hosts are listed in the index.
#                    The function identifies hosts that are expected (in the mesh configuration)
#                    but not found in Elasticsearch.
#
#                    The process retrieves the hosts from the configuration, queries Elasticsearch
#                    for the relevant test data, and counts the number of hosts not found.
#                    In addition, the function can generate a plot comparing the number of hosts found
#                    in the configuration versus those found in Elasticsearch. This information helps
#                    maintain an accurate and up-to-date monitoring system by identifying discrepancies
#                    between the expected and actual data.
#                    TODO: Marian's API returns sites whose names do not exactly match those recorded in Elasticsearch. This directly affects the tags that users use.
# Author: Yana Holoborodko
# Copyright 2024
import helpers as hp
import warnings
import time
import datetime as dt
import hashlib
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import psconfig.api
import urllib3
from alarms import alarms

def query4Hosts(dateFrom, dateTo, testType):
    query = {
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "gt" : dateFrom,
                      "lte": dateTo
                    }
                  }
                }
              ]
            }
          }
    aggregations = {
        "unique_host_pairs": {
            "composite": {
                "size": 20000,
                "sources": [
                    {
                        "src_host": {
                            "terms": {
                                "field": "src_host"
                            }
                        }
                    },
                    {
                        "dest_host": {
                            "terms": {
                                "field": "dest_host"
                            }
                        }
                    }
                ]
            }
        }
    }
    aggrs = []
    aggdata = hp.es.search(index=f'ps_{testType}', query=query, aggregations=aggregations)
    for item in aggdata['aggregations']['unique_host_pairs']['buckets']:
        aggrs.append(item['key']['src_host'])
        aggrs.append(item['key']['dest_host'])
    return aggrs

def queryData(dateFrom, dateTo, test):
    data = []
    # query in portions since ES does not allow aggregations with more than 10000 bins
    intv = int(hp.CalcMinutes4Period(dateFrom, dateTo)/60)
    time_list = hp.GetTimeRanges(dateFrom, dateTo, intv)
    for i in range(len(time_list)-1):
        data.extend(query4Hosts(time_list[i], time_list[i+1], test))
    return set(data)

def check_tests_for_host(host, mesh_config):
    """
    Classifies the host as belonging to one of the three test groups (latency, trace and throughput).
    """
    try:
        types = mesh_config.get_test_types(host)
    except Exception:
        return False, False
    latency = any(test in ['latency', 'latencybg'] for test in types)
    trace = 'trace' in types
    throughput = any(test in ['throughput', 'rtt'] for test in types) # as rtt is now in ps_throughput
    return host, latency, trace, throughput

def create_hosts_tests_types_grid(hosts, mesh_config):
    """
    Creates a dataframe with a list of all hosts and whether
    or not they are tested in each group(latency and trace).
    """
    host_test_type = pd.DataFrame({
    'host': list(hosts),
    'owd': False,
    'trace': False,
    'throughput': False
    })
    host_test_type = host_test_type['host'].apply(
        lambda host: pd.Series(check_tests_for_host(host, mesh_config))
    )
    host_test_type.columns = ['host', 'owd', 'trace', 'throughput']
    return host_test_type

def check_data_difference_in_es(data_from, data_to, test_type, expected_hosts):
    """
    Checks whether all expected(mentioned in configurations) hosts
    were found in the Elasticsearch, and returns the list of hosts which were omitted.
    Can creates the plot for visualization of results.
    """
    data = queryData(data_from, data_to, test_type)
    difference = expected_hosts.difference(data)
    p = len(difference)/len(expected_hosts) * 100
    return difference, round(p, 2)

if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    today_date = dt.date.today()
    delta = today_date - dt.timedelta(days=2)
    time_from = dt.time(0, 0)
    time_to = dt.time(23, 59, 59)
    m_from = dt.datetime.combine(delta, time_from).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    m_to = dt.datetime.combine(delta, time_to).strftime('%Y-%m-%dT%H:%M:%S.000Z')
    mesh_url = "https://psconfig.aglt2.org/pub/config"
    mesh_config = psconfig.api.PSConfig(mesh_url)
    all_hosts = mesh_config.get_all_hosts()
    ips = list(all_hosts)
    test_types = ['owd', 'trace', 'throughput']
    expected_tests_types = create_hosts_tests_types_grid(ips, mesh_config)
    sites_mapping = {}
    stats = {'percent_owd': None,
            'percent_trace': None,
            'percent_throughput': None,
            'num_not_found_owd': None,
            'num_not_found_trace': None,
            'num_not_found_throughput': None,
            'num_expected_owd': None,
            'num_expected_trace': None,
            'num_expected_throughput': None}
    for test in test_types:
        expected_hosts_test = set(expected_tests_types[expected_tests_types[test] == True]['host'].to_list())
        diff, percent = check_data_difference_in_es(m_from, m_to, test, expected_hosts_test)
        for host in diff:
            site = mesh_config.get_site(host)
            if site not in sites_mapping:
                type_subset = dict()
                sites_mapping[site] = type_subset
            if test not in sites_mapping[site].keys():
                sites_mapping[site][test] = set()
            sites_mapping[site][test].add(host)
        stats[f'percent_{test}'] = percent
        stats[f'num_not_found_{test}'] = len(diff)
        stats[f'num_expected_{test}'] = len(expected_hosts_test)
    # print(f"Hosts expected but not found in the Elasticsearch ps-owd({stats['percent_owd']}% ({stats['num_not_found_owd']}/{stats['num_expected_owd']}) out of included to configurations not found)\nHosts expected but not found in the Elasticsearch ps-trace({stats['percent_trace']}% ({stats['num_not_found_trace']}/{stats['num_expected_trace']}) out of included to configurations not found)\nHosts expected but not found in the Elasticsearch ps-owd({stats['percent_throughput']}% ({stats['num_not_found_throughput']}/{stats['num_expected_throughput']}) out of included to configurations not found)\n")
    for s in sites_mapping.keys():
        alarmOnSite = alarms('Networking', 'Sites', f"hosts not found")
        doc = {'from': m_from,
               'to': m_to,
               'site': s,
               'hosts': sites_mapping[s]}
        toHash = ','.join([s, str(sites_mapping[s]), m_from, m_to, test])
        doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
        alarmOnSite.addAlarm(body='not found in the Elasticsearch', tags=[s], source=doc)
        # print(f"Hosts expected but not found in the Elasticsearch\n{s}\n{doc['hosts']}\n")
