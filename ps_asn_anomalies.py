import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from collections import defaultdict
import pyarrow as pa
import psutil
import hashlib
import logging
from typing import List, Tuple, Dict, Any
import time

from alarms import alarms
from utils.helpers import timer
import utils.helpers as hp

# Constants
INTERVAL_HOURS = 2
BATCH_SIZE = 1000  # Adjusted based on memory usage
MAX_THREADS_MULTIPLIER = 1.5  # Adjusted based on CPU usage
DAYS_BACK = 6

# Configure logging
logging.basicConfig(level=logging.WARNING)  # Set to WARNING to reduce output
logger = logging.getLogger(__name__)

# Suppress elastic_transport logging
logging.getLogger('elastic_transport.transport').setLevel(logging.ERROR)  # Set to ERROR to reduce output

def build_query(start_time: str, end_time: str) -> Dict[str, Any]:
    """Builds the Elasticsearch query."""
    return {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gt": start_time,
                                "lte": end_time,
                                "format": "strict_date_optional_time"
                            }
                        }
                    }
                ]
            }
        },
        "runtime_mappings": {
            "asn_path": {
                "type": "keyword",
                "script": {
                    "source": """
                    if (params._source.asns.size() >= 3) {
                        boolean allZeros = true;
                        for (int asn : params._source.asns) {
                            if (asn != 0) {
                                allZeros = false;
                                break;
                            }
                        }
                        if (!allZeros) {
                            List strings = new ArrayList();
                            for (int asn : params._source.asns) {
                                strings.add(asn.toString());
                            }
                            emit(String.join("-", strings));
                        }
                    }
                    """
                }
            },
            "ip_path": {
                "type": "keyword",
                "script": {
                    "source": """
                    if (params._source.hops != null && params._source.hops.size() > 0) {
                        List ips = new ArrayList();
                        for (String hop : params._source.hops) {
                            ips.add(hop);
                        }
                        emit(String.join("->", ips));
                    }
                    """
                }
            }
        },
        "aggs": {
            "unique_paths": {
                "composite": {
                    "size": 10000,
                    "sources": [
                        {"asn_path": {"terms": {"field": "asn_path"}}},
                        {"ip_path": {"terms": {"field": "ip_path"}}},
                        {"src_netsite": {"terms": {"field": "src_netsite"}}},
                        {"dest_netsite": {"terms": {"field": "dest_netsite"}}},
                        {"ipv6": {"terms": {"field": "ipv6"}}}
                    ]
                }
            }
        }
    }

def adjust_date_by_days_now(days: int, fixed_date: datetime = datetime.now(timezone.utc)) -> str:
    """Adjusts the date by a given number of days."""
    adjusted_date_obj = fixed_date + timedelta(days=days)
    adjusted_date_str = adjusted_date_obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
    return adjusted_date_str

def generate_time_ranges(start_time: str, end_time: str, interval_hours: int = INTERVAL_HOURS) -> List[Tuple[str, str]]:
    """Generates time ranges between start and end times."""
    start = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    ranges = [
        (
            (start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            (min(start + timedelta(hours=i + interval_hours), end)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        for i in range(0, int((end - start).total_seconds() // 3600) + 1, interval_hours)
    ]
    return ranges

def query_and_paginate(start_time: str, end_time: str) -> List[Dict[str, Any]]:
    """Queries and paginates the results from Elasticsearch."""
    query = build_query(start_time, end_time)
    after_key = None
    results = []
    while True:
        if after_key:
            query["aggs"]["unique_paths"]["composite"]["after"] = after_key
        response = hp.es.search(index="ps_trace", body=query)
        buckets = response["aggregations"]["unique_paths"]["buckets"]
        for bucket in buckets:
            results.append({
                "asn_path": bucket["key"]["asn_path"],
                "ip_path": bucket["key"]["ip_path"],
                "src_netsite": bucket["key"]["src_netsite"].upper(),
                "dest_netsite": bucket["key"]["dest_netsite"].upper(),
                "ipv6": bucket["key"]["ipv6"],
                "doc_count": bucket["doc_count"],
                "dt": end_time
            })
        after_key = response["aggregations"]["unique_paths"].get("after_key")
        if not after_key:
            break
    return results

def parallel_querying_with_threads(time_ranges: List[Tuple[str, str]], max_threads: int) -> pd.DataFrame:
    """Performs parallel querying with threads."""
    results = []
    with tqdm(total=len(time_ranges), desc="Querying Time Ranges", dynamic_ncols=True) as pbar:
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_to_time = {executor.submit(query_and_paginate, start, end): (start, end)
                              for start, end in time_ranges}
            for future in as_completed(future_to_time):
                time_range = future_to_time[future]
                try:
                    results.extend(future.result())
                except Exception as exc:
                    print(f"Time range {time_range} generated an exception: {exc}")
                finally:
                    pbar.update(1)
    return pd.DataFrame(results)

def generate_ip_to_asn_mapping_batch(asn_paths: List[List[int]], ip_paths: List[List[str]]) -> Dict[str, set]:
    """Generates IP to ASN mapping for a batch."""
    ip_to_asn = defaultdict(set)
    for asn_path, ip_path in zip(asn_paths, ip_paths):
        for asn, ip in zip(asn_path, ip_path):
            if asn != 0:
                ip_to_asn[ip].add(asn)
    return ip_to_asn

def map_ip_to_asn(df: pd.DataFrame, max_threads: int, batch_size: int = BATCH_SIZE) -> Dict[str, set]:
    """Combines IP to ASN mappings."""
    combined_mapping = defaultdict(set)
    def process_batch(batch_df: pd.DataFrame) -> Dict[str, set]:
        return generate_ip_to_asn_mapping_batch(
            batch_df["asn_path_list"].tolist(),
            batch_df["ip_path_list"].tolist()
        )
    batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
    results = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        with tqdm(total=len(batches), desc="Mapping IP to ASN(s)") as pbar:
            for result in executor.map(process_batch, batches):
                results.append(result)
                pbar.update(1)
    for batch_mapping in results:
        for ip, asns in batch_mapping.items():
            combined_mapping[ip].update(asns)
    return dict(combined_mapping)

def create_asn_to_ip_path(asn_path_list: List[int], ip_path_list: List[str]) -> List[str]:
    """Creates ASN to IP path."""
    return [f"{asn}({ip})" for asn, ip in zip(asn_path_list, ip_path_list)]

def repair_asn_path(asn_path_list: List[int], ip_path_list: List[str], ip_to_asn_mapping: Dict[str, set]) -> Tuple[List[int], int, bool]:
    """Repairs ASN path using IP to ASN mapping."""
    repaired_path = []
    all_repaired = True
    for asn, ip in zip(asn_path_list, ip_path_list):
        if asn == 0:
            possible_asns = ip_to_asn_mapping.get(ip, set())
            if len(possible_asns) == 1:
                repaired_path.append(next(iter(possible_asns)))
            else:
                repaired_path.append(0)
                all_repaired = False
        else:
            repaired_path.append(asn)
    path_len = len(repaired_path)
    return repaired_path, path_len, all_repaired

def repair_ASN0_in_batches(df: pd.DataFrame, ip_to_asn_mapping: Dict[str, set], max_threads: int, batch_size: int = BATCH_SIZE) -> pd.DataFrame:
    """Try to repair the 0 ASNs by using the IP addresses at."""
    def process_batch(batch: pd.DataFrame) -> List[Tuple[List[int], int, bool, List[str], bool]]:
        repaired_results = []
        for _, row in batch.iterrows():
            has_zeros = 0 in row["asn_path_list"]
            repaired_path, path_len, all_repaired = repair_asn_path(
                row["asn_path_list"], row["ip_path_list"], ip_to_asn_mapping
            )
            asn_to_ip_path = create_asn_to_ip_path(
                repaired_path, row["ip_path_list"]
            )
            repaired_results.append((repaired_path, path_len, all_repaired, asn_to_ip_path, has_zeros))
        return repaired_results
    results = []
    batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
    with tqdm(total=len(batches), desc="Repair ASNs via positional IPs") as pbar:
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for batch_results in executor.map(process_batch, batches):
                results.extend(batch_results)
                pbar.update(1)
    repaired_paths, path_lengths, all_repaired_flags, asn_to_ip_paths, has_zeros_list = zip(*results)
    results_df = pd.DataFrame({
        "repaired_asn_path": repaired_paths,
        "path_len": path_lengths,
        "all_repaired": all_repaired_flags,
        "asn_to_ip_path": asn_to_ip_paths,
        "has_zeros": has_zeros_list
    })
    df = pd.concat([df.reset_index(drop=True), results_df], axis=1)
    return df

def process_group(group_info: pd.Series, df: pd.DataFrame) -> pd.DataFrame:
    """Processes a group of data."""
    src, dest, ipv, doc_count = group_info['src_netsite'], group_info['dest_netsite'], group_info['ipv6'], group_info['doc_count']
    subset = df[(df['src_netsite'] == src) & (df['dest_netsite'] == dest) & (df['ipv6'] == ipv)].copy().reset_index()
    max_length = subset["path_len"].max()
    pivot_df = pd.DataFrame(
        subset["repaired_asn_path"].tolist(),
        index=subset.index,
        columns=[f"asn_{i+1}" for i in range(max_length)]
    )
    unique_rids = pd.Series(pivot_df.stack().unique()).dropna().tolist()
    value_counts_per_row = pivot_df.apply(lambda row: row.value_counts(), axis=1).fillna(0)
    first_last_appearance = {
        rid: {
            "first_appearance": subset.loc[pivot_df.isin([rid]).any(axis=1), "dt"].min(),
            "last_appearance": subset.loc[pivot_df.isin([rid]).any(axis=1), "dt"].max(),
        }
        for rid in unique_rids
    }
    last_non_null_asn = pivot_df.apply(lambda row: row.dropna().iloc[-1], axis=1)
    last_asn_counts = last_non_null_asn.value_counts()
    asn_last_freq = {asn: last_asn_counts.get(asn, 0) / len(last_non_null_asn) for asn in unique_rids}
    result_df = pd.DataFrame({
        "src_netsite": src,
        'dest_netsite': dest,
        "ipv6": ipv,
        "num_tests_pair": doc_count,
        "asn": unique_rids,
        "asn_total_count": value_counts_per_row.sum(axis=0).reindex(unique_rids, fill_value=0),
        "on_path": [
            pivot_df.isin([rid]).any(axis=1).mean() for rid in unique_rids
        ],
        "first_appearance": [first_last_appearance[rid]["first_appearance"] for rid in unique_rids],
        "last_appearance": [first_last_appearance[rid]["last_appearance"] for rid in unique_rids],
        "positioned_last_freq": [asn_last_freq.get(rid, 0) for rid in unique_rids],
    })
    return result_df

def process_batches(site_groups: pd.DataFrame, df: pd.DataFrame, batch_size: int = 5, workers: int = 5) -> pd.DataFrame:
    """Processes data in batches."""
    global_results = pd.DataFrame()
    batches = [site_groups[i:i + batch_size] for i in range(0, len(site_groups), batch_size)]
    for batch in tqdm(batches, desc="Process pairs in groups"):
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(process_group, row,
                                       df[(df['src_netsite'] == row['src_netsite'])\
                                       & (df['dest_netsite'] == row['dest_netsite'])\
                                       & (df['ipv6'] == row['ipv6'])].copy()) for _, row in batch.iterrows()]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    global_results = pd.concat([global_results, result], ignore_index=True)
                except Exception as e:
                    print(f"Error processing batch: {e}")
    return global_results

def detect_and_send_anomalies(asn_stats: pd.DataFrame, start_date: str) -> None:
    """Detects anomalies in ASN paths."""
    asn_stats['asn'] = asn_stats['asn'].astype(int)
    current_date = datetime.now(timezone.utc)
    threshold_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    anomalies = asn_stats[(asn_stats['on_path'] < 0.3) &
                        (asn_stats['asn'] > 0) &
                        (asn_stats['positioned_last_freq'] == 0) & \
                        (asn_stats['first_appearance'] > threshold_date)]

    # get the exact number of tests
    anomalies = anomalies.assign(on_path_count=anomalies['num_tests_pair'] * anomalies['on_path'])

    # consider and ASN anomalous only if the an anomaly was seen more then 3 times
    anomalies = anomalies[(anomalies['on_path_count'] > 3) & (anomalies['num_tests_pair'] > 10)]

    # consider and ASN anomalous only if the an anomaly was seen more then 2 times
    possible_anomalous_pairs = anomalies[(anomalies['on_path_count'] > 2)]\
                                .groupby(['src_netsite', 'dest_netsite','ipv6'])\
                                .agg(
                                    asn_count=('asn', 'count'),
                                    asn_list=('asn', list)
                                ).reset_index()

    possible_anomalous_pairs['ipv'] = possible_anomalous_pairs['ipv6'].apply(lambda x: 'IPv6' if x else 'IPv4')
    possible_anomalous_pairs['to_date'] = start_date

    if len(possible_anomalous_pairs)==0:
      print('No unusual ASNs observed in the past day.')
    else:
      ALARM = alarms('Networking', 'RENs', 'ASN path anomalies')
      for doc in possible_anomalous_pairs.to_dict('records'):
          tags = [doc['src_netsite'], doc['dest_netsite']]
          toHash = ','.join([doc['src_netsite'], doc['dest_netsite'], str(current_date)])
          alarm_id = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
          doc['alarm_id'] = alarm_id
          print(f"Detected anomaly: {doc}")
          ALARM.addAlarm(
                  body="Path anomaly detected",
                  tags=tags,
                  source=doc
              )

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the data."""
    df["asn_path_list"] = df["asn_path"].apply(lambda x: [int(i) for i in x.split('-')])
    df["ip_path_list"] = df["ip_path"].str.split('->')
    return df[['asn_path', 'ip_path', 'dt', 'asn_path_list', 'ip_path_list']].drop_duplicates(subset=['asn_path', 'ip_path'])

def group_site_data(df: pd.DataFrame) -> pd.DataFrame:
    """Groups site data in order to reduce resources."""
    return df[['src_netsite', 'dest_netsite', 'ipv6', 'dt', 'doc_count']].groupby(
        ['src_netsite', 'dest_netsite', 'ipv6']
    ).agg({'doc_count': 'sum', 'dt': 'count'}).reset_index()

def monitor_resources(interval=15):
    cpu_usage = []
    memory_usage = []
    disk_usage = []
    network_sent = []
    network_received = []

    while True:
        cpu_usage.append(psutil.cpu_percent(interval=interval))
        memory_info = psutil.virtual_memory()
        disk_info = psutil.disk_usage('/')
        network_info = psutil.net_io_counters()

        memory_usage.append(memory_info.percent)
        disk_usage.append(disk_info.percent)
        network_sent.append(network_info.bytes_sent)
        network_received.append(network_info.bytes_recv)

        time.sleep(interval)

        # Stop monitoring after a certain period (e.g., 60 seconds)
        if len(cpu_usage) >= 60:
            break

    # Calculate averages
    avg_cpu_usage = sum(cpu_usage) / len(cpu_usage)
    avg_memory_usage = sum(memory_usage) / len(memory_usage)
    avg_disk_usage = sum(disk_usage) / len(disk_usage)
    total_network_sent = network_sent[-1] - network_sent[0]
    total_network_received = network_received[-1] - network_received[0]

    # Print summary
    print(f"Average CPU Usage: {avg_cpu_usage:.2f}%")
    print(f"Average Memory Usage: {avg_memory_usage:.2f}%")
    print(f"Average Disk Usage: {avg_disk_usage:.2f}%")
    print(f"Total Network Sent: {total_network_sent / (1024 * 1024):.2f} MB")
    print(f"Total Network Received: {total_network_received / (1024 * 1024):.2f} MB")

# Run the monitor in a separate thread
import threading
monitor_thread = threading.Thread(target=monitor_resources)
monitor_thread.start()

@timer
def main():
    try:
        num_cores = os.cpu_count()
        max_threads = int((num_cores) * MAX_THREADS_MULTIPLIER)
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        start_date = (datetime.now(timezone.utc)- timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        time_ranges = generate_time_ranges(start_date, end_date, interval_hours=INTERVAL_HOURS)
        df = parallel_querying_with_threads(time_ranges, max_threads)

        agg_df = process_data(df)
        ip_to_asn_mapping = map_ip_to_asn(agg_df, max_threads, batch_size=BATCH_SIZE)
        df = repair_ASN0_in_batches(df, ip_to_asn_mapping, max_threads=8, batch_size=BATCH_SIZE)

        site_groups = group_site_data(df)
        asn_stats = process_batches(site_groups, df, batch_size=50, workers=10)

        detect_and_send_anomalies(asn_stats, start_date)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()