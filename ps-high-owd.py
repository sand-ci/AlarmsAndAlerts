"""
High One-Way Delay Detection with Adaptive Baselines

This module detects high delay events using 7-days baseline-based adaptive thresholds
and handles pairs with clock synchronization issues.
"""

import hashlib
import pandas as pd
import numpy as np
from alarms import alarms
import utils.helpers as hp
from utils.helpers import timer
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from datetime import datetime, timezone, timedelta
import warnings
warnings.filterwarnings('ignore')

def get_expected_owd(src_site, dest_site, time_window=7, field_type='netsite', reference_date=None):
    """
    Calculate expected OWD baseline for a site pair over a time window ending at reference_date.
    Returns dict with owd_stats: avg, p95, iqr, etc.
    """
    # If reference_date is not provided, use now
    if reference_date is None:
        reference_date = datetime.utcnow()

    # Query time window: [reference_date - time_window, reference_date)
    date_to = reference_date
    date_from = date_to - timedelta(days=time_window)

    # Query OWD data for the baseline period
    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "timestamp": {
                            "gt": int(date_from.timestamp() * 1000),
                            "lte": int(date_to.timestamp() * 1000)
                        }
                    }
                },
                {"term": {"src_production": True}},
                {"term": {"dest_production": True}},
                {"term": {"src_netsite" if field_type == "netsite" else "src_site": src_site}},
                {"term": {"dest_netsite" if field_type == "netsite" else "dest_site": dest_site}}
            ]
        }
    }
    aggregations = {
        "delay_stats": {
            "stats": {"field": "delay_median"}
        },
        "delay_percentiles": {
            "percentiles": {
                "field": "delay_median",
                "percents": [50, 75, 90, 95, 99]
            }
        }
    }
    result = hp.es.search(index='ps_owd', query=query, aggregations=aggregations)
    stats = result.get('aggregations', {}).get('delay_stats', {})
    percentiles = result.get('aggregations', {}).get('delay_percentiles', {}).get('values', {})

    # Defensive: If percentiles are missing, set to None
    def safe_get(d, key):
        v = d.get(key)
        # if v is None: print(f"[WARNING] Missing {key} in data for {src_site} -> {dest_site}")
        return v if v is not None else None

    # Calculate IQR (75th - 50th percentile)
    iqr = None
    p75 = safe_get(percentiles, '75.0')
    p50 = safe_get(percentiles, '50.0')
    if p75 is not None and p50 is not None:
        iqr = p75 - p50

    # Diagnostics for missing baseline data
    missing_fields = []
    for field, val in [('avg', safe_get(stats, 'avg')),
                       ('p95', safe_get(percentiles, '95.0')),
                       ('iqr', iqr),
                       ('median', p50)]:
        if val is None:
            missing_fields.append(field)
    # if missing_fields:
    #     print(f"[BASELINE DIAGNOSTICS] Missing baseline fields for {src_site} -> {dest_site}: {missing_fields}")
    #     print(f"  Raw stats: {stats}")
    #     print(f"  Raw percentiles: {percentiles}")

    owd_stats = {
        'avg': safe_get(stats, 'avg'),
        'p95': safe_get(percentiles, '95.0'),
        'iqr': iqr,
        'median': p50,
        'max': safe_get(stats, 'max'),
        'min': safe_get(stats, 'min')
    }
    return {'owd_stats': owd_stats}


def analyze_delay_distribution(aggrs):
    """Analyze delay distribution to understand negative delays and outliers"""
    if not aggrs:
        return
    
    # Filter out None values first
    delays = [item['delay_mean'] for item in aggrs if item['delay_mean'] is not None]
    delays_p95 = [item['delay_p95'] for item in aggrs if item['delay_p95'] is not None]
    
    # Count null values
    null_mean_count = sum(1 for item in aggrs if item['delay_mean'] is None)
    null_p95_count = sum(1 for item in aggrs if item['delay_p95'] is None)
    
    print(f"\n📊 DELAY DISTRIBUTION ANALYSIS:")
    print(f"   Total measurements: {len(aggrs)}")
    if null_mean_count > 0 or null_p95_count > 0:
        print(f"   Null mean delays: {null_mean_count}")
        print(f"   Null P95 delays: {null_p95_count}")
    
    if not delays:
        print("   No valid delay data found")
        return
        
    negative_delays = [d for d in delays if d < 0]
    negative_p95 = [d for d in delays_p95 if d < 0]
    
    print(f"   Valid mean delays: {len(delays)}")
    print(f"   Negative mean delays: {len(negative_delays)} ({len(negative_delays)/len(delays)*100:.1f}%)")
    if delays_p95:
        print(f"   Negative P95 delays: {len(negative_p95)} ({len(negative_p95)/len(delays_p95)*100:.1f}%)")
    
    if negative_delays:
        print(f"   Worst negative mean: {min(negative_delays):.1f}ms")
        print(f"   Best negative mean: {max(negative_delays):.1f}ms")
    
    # Clock sync issues vs minor negative delays
    extreme_negative = [d for d in delays if d < -1000]  # Likely clock sync issues
    minor_negative = [d for d in delays if -1000 <= d < 0]  # Likely measurement noise
    
    print(f"   Extreme negative (<-1000ms): {len(extreme_negative)} (likely clock sync issues)")
    print(f"   Minor negative (-1000 to 0ms): {len(minor_negative)} (likely measurement noise)")
    
    # Positive delay stats for context
    positive_delays = [d for d in delays if d > 0]
    if positive_delays:
        print(f"   Positive delay range: {min(positive_delays):.1f} to {max(positive_delays):.1f}ms")
        print(f"   Median positive delay: {np.median(positive_delays):.1f}ms")


def query_owd_data(date_from: str, date_to: str):
    """Query OWD data with proper filtering and analysis"""
    
    # Convert time strings to epoch milliseconds for ES query
    from datetime import datetime
    import time
    
    # Parse the time strings and convert to UTC epoch milliseconds
    date_from_dt = datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S.%f')
    date_to_dt = datetime.strptime(date_to, '%Y-%m-%d %H:%M:%S.%f')

    date_from_ms = int(date_from_dt.timestamp() * 1000)
    date_to_ms = int(date_to_dt.timestamp() * 1000)
    
    print(f"🔍 Querying OWD data: {date_from} to {date_to}")
    print(f"   Epoch range: {date_from_ms} to {date_to_ms}")
    
    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "timestamp": {
                            "gt": date_from_ms,
                            "lte": date_to_ms
                        }
                    }
                },
                {
                    "term": {
                        "src_production": True
                    }
                },
                {
                    "term": {
                        "dest_production": True
                    }
                }
            ]
        }
    }

    aggregations = {
        "groupby": {
            "composite": {
                "size": 9999,
                "sources": [
                    {"ipv6": {"terms": {"field": "ipv6"}}},
                    {"src": {"terms": {"field": "src"}}},
                    {"dest": {"terms": {"field": "dest"}}},
                    {"src_host": {"terms": {"field": "src_host"}}},
                    {"dest_host": {"terms": {"field": "dest_host"}}},
                    {"src_site": {"terms": {"field": "src_netsite"}}},
                    {"dest_site": {"terms": {"field": "dest_netsite"}}}
                ]
            },
            "aggs": {
                "delay_stats": {
                    "stats": {"field": "delay_median"}
                },
                "delay_percentiles": {
                    "percentiles": {
                        "field": "delay_median",
                        "percents": [50, 75, 90, 95, 99]
                    }
                }
            }
        }
    }

    aggrs = []
    aggdata = hp.es.search(index='ps_owd', query=query, aggregations=aggregations)
    
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({
            'pair': str(item['key']['src'] + '-' + item['key']['dest']),
            'from': date_from, 'to': date_to,
            'ipv6': item['key']['ipv6'],
            'src': item['key']['src'], 'dest': item['key']['dest'],
            'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
            'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
            'delay_mean': item['delay_stats']['avg'],
            'delay_median': item['delay_percentiles']['values']['50.0'],  # Add median
            'delay_max': item['delay_stats']['max'],
            'delay_min': item['delay_stats']['min'],
            'delay_p95': item['delay_percentiles']['values']['95.0'],
            'delay_p99': item['delay_percentiles']['values']['99.0'],
            'doc_count': item['doc_count']
        })

    # Analyze delay distribution before filtering
    analyze_delay_distribution(aggrs)
    
    return aggrs


def process_single_pair(row_data, date_from, date_to):
    """Process a single pair for baseline comparison - designed for parallel execution"""
    try:

        # Convert date_from to datetime for baseline reference
        reference_date = datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S.%f')

        # Get baseline using get_expected_owd with reference date
        baseline = get_expected_owd(
            row_data['src_site'],
            row_data['dest_site'],
            time_window=7,
            field_type='netsite',
            reference_date=reference_date
        )

        if baseline is None or baseline.get('owd_stats') is None:
            return None

        # Extract statistics from baseline
        owd_stats = baseline['owd_stats']
        baseline_p95 = owd_stats.get('p95')
        baseline_mean = owd_stats.get('avg')
        baseline_iqr = owd_stats.get('iqr')

        # Defensive: skip if any required baseline stats are None
        if baseline_p95 is None or baseline_mean is None or baseline_iqr is None:
            return None

        # Adaptive threshold: 1.5x baseline P95 or baseline + 2*IQR, whichever is higher
        adaptive_threshold = max(
            baseline_p95 * 1.5,
            baseline_mean + (2 * baseline_iqr)
        )

        # Defensive: skip if current delay values are None
        if row_data.get('use_median', False):
            current_delay = row_data['delay_median']
            current_p95 = row_data['delay_p95']
            delay_type = 'median'
        else:
            current_delay = row_data['delay_mean']
            current_p95 = row_data['delay_p95']
            delay_type = 'mean'

        if current_delay is None or current_p95 is None:
            return None

        # Check if current delay exceeds adaptive threshold
        if current_p95 > adaptive_threshold:
            # Calculate severity
            severity_multiplier = current_p95 / baseline_p95

            return {
                'src_site': row_data['src_site'],
                'dest_site': row_data['dest_site'],
                'src_host': row_data['src_host'],
                'dest_host': row_data['dest_host'],
                'ipv6': row_data['ipv6'],
                'current_delay_p95': round(current_p95, 2),
                'current_delay_metric': round(current_delay, 2),
                'delay_type_used': delay_type,
                'baseline_p95': round(baseline_p95, 2),
                'threshold': round(adaptive_threshold, 2),
                'severity_multiplier': round(severity_multiplier, 2),
                'doc_count': row_data['doc_count'],
                'has_negative_mean': row_data.get('use_median', False),
                'from': date_from,
                'to': date_to
            }

        return None  # No anomaly detected

    except Exception as e:
        print(f"Error processing {row_data.get('src_site', '')} -> {row_data.get('dest_site', '')}: {e}")
        return None


@timer
def detect_high_owd_with_baselines(date_from: str, date_to: str, max_workers=10):
    """Detect high OWD using adaptive baselines with parallel processing"""
    
    # Query recent OWD data
    owd_data = query_owd_data(date_from, date_to)
    df = pd.DataFrame(owd_data)
    
    if df.empty:
        print("No OWD data found")
        return pd.DataFrame()
    
    # Clean data with improved negative delay handling
    print(f"\n🧹 FILTERING DATA:")
    original_count = len(df)
    
    # Step 0: Remove null values first
    null_mean_mask = df['delay_mean'].isnull()
    null_p95_mask = df['delay_p95'].isnull()
    null_median_mask = df['delay_median'].isnull()
    
    null_count = (null_mean_mask | null_p95_mask | null_median_mask).sum()
    if null_count > 0:
        print(f"   Removing {null_count} pairs with null delay values")
    
    # Step 1: Remove extreme outliers (likely clock sync issues)
    extreme_negative_mask = (df['delay_mean'] <= -1000) & ~null_mean_mask
    extreme_positive_mask = (df['delay_mean'] >= 50000) & ~null_mean_mask  # Very high delays
    min_measurements_mask = df['doc_count'] < 10
    
    extreme_negative_count = extreme_negative_mask.sum()
    extreme_positive_count = extreme_positive_mask.sum() 
    min_measurements_count = min_measurements_mask.sum()
    
    print(f"   Removing {extreme_negative_count} pairs with extreme negative delays (<= -1000ms)")
    print(f"   Removing {extreme_positive_count} pairs with extreme positive delays (>= 50000ms)")
    print(f"   Removing {min_measurements_count} pairs with insufficient measurements (< 10)")
    
    # Apply filtering
    df_filtered = df[
        ~null_mean_mask & 
        ~null_p95_mask & 
        ~null_median_mask & 
        ~extreme_negative_mask & 
        ~extreme_positive_mask & 
        ~min_measurements_mask
    ].copy()
    
    # Step 2: Handle minor negative delays (-1000 to 0ms)
    minor_negative_mask = (df_filtered['delay_mean'] < 0)
    minor_negative_count = minor_negative_mask.sum()
    
    print(f"   Pairs with minor negative delays (-1000 to 0ms): {minor_negative_count}")
    
    if minor_negative_count > 0:
        # For minor negative delays, use median and P95 instead of mean
        print(f"   → Will use median/P95 for analysis instead of mean for negative delay pairs")
        # Mark these pairs for special handling
        df_filtered.loc[minor_negative_mask, 'use_median'] = True
        df_filtered.loc[~minor_negative_mask, 'use_median'] = False
    else:
        df_filtered['use_median'] = False
    
    filtered_count = len(df_filtered)
    print(f"   Data points: {original_count} → {filtered_count} (removed {original_count - filtered_count})")
    
    df = df_filtered
    
    print(f"📊 Processing {len(df)} pairs for baseline comparison using {max_workers} workers")
    
    # Initialize BaselineManager (will be used by each worker)
    # baseline_manager = BaselineManager()
    
    # Convert rows to dictionaries for parallel processing
    row_dicts = [row.to_dict() for _, row in df.iterrows()]
    
    # Create partial function with fixed parameters
    process_func = partial(process_single_pair, 
                          date_from=date_from, 
                          date_to=date_to)
    
    anomalous_delays = []
    
    # Process pairs in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_pair = {executor.submit(process_func, row_dict): row_dict['src_site'] + '->' + row_dict['dest_site'] 
                         for row_dict in row_dicts}
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_pair):
            pair_name = future_to_pair[future]
            try:
                result = future.result()
                if result is not None:
                    anomalous_delays.append(result)
                
                completed += 1
                if completed % 500 == 0:  # Progress indicator
                    print(f"   Processed {completed}/{len(row_dicts)} pairs...")
                    
            except Exception as e:
                print(f"   Error processing {pair_name}: {e}")
    
    print(f"✅ Completed processing. Found {len(anomalous_delays)} anomalous delays")

    return pd.DataFrame(anomalous_delays)


def find_multi_site_delay_issues(anomalous_df, threshold=5):
    """Identify sites with high delay to/from multiple destinations"""
    if anomalous_df.empty:
        return []
    
    # Count issues per site as source
    src_counts = anomalous_df['src_site'].value_counts()
    # Count issues per site as destination  
    dest_counts = anomalous_df['dest_site'].value_counts()
    
    # Combine counts
    all_counts = src_counts.add(dest_counts, fill_value=0)
    
    # Return sites with issues >= threshold
    return all_counts[all_counts >= threshold].index.tolist()


def convert_to_iso_format(date_str):
    """Convert date string to ISO 8601 format 'YYYY-MM-DDTHH:MM:SS.000Z' for ES compatibility."""
    from datetime import datetime
    # Try parsing with known formats
    for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Always output with seconds and Z
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except Exception:
            continue
    # If parsing fails, return original string
    return date_str

def send_high_owd_alarms(anomalous_df, test_mode=False):
    """Send appropriate alarms for high OWD events"""
    if anomalous_df.empty:
        print("No high OWD anomalies detected")
        if test_mode:
            return pd.DataFrame()  # Return empty dataframe in test mode
        return
        
    if test_mode:
        print("\n🧪 TEST MODE: Displaying results instead of sending alarms")
        print("=" * 60)
        
    # Create alarm objects (only if not in test mode)
    if not test_mode:
        alarm_pair = alarms('Networking', 'Network', 'high one-way delay')
        alarm_multi = alarms('Networking', 'Network', 'high delay from/to multiple sites')
    
    # Find multi-site issues
    multi_site_issues = find_multi_site_delay_issues(anomalous_df)
    
    print(f"High OWD anomalies detected: {len(anomalous_df)}")
    print(f"Multi-site delay issues: {len(multi_site_issues)}")
    
    # Store all alarm data for test mode
    all_alarm_data = []
    
    # Handle multi-site issues first
    for site in multi_site_issues:
        site_issues = anomalous_df[
            (anomalous_df['src_site'] == site) | 
            (anomalous_df['dest_site'] == site)
        ]
        
        dest_sites = site_issues[site_issues['src_site'] == site]['dest_site'].tolist()
        src_sites = site_issues[site_issues['dest_site'] == site]['src_site'].tolist()

        # Calculate average severity
        avg_severity = site_issues['severity_multiplier'].mean()
        max_delay = site_issues['current_delay_p95'].max()
        
        to_hash = ','.join([site, str(max_delay), anomalous_df.iloc[0]['from'], anomalous_df.iloc[0]['to']])
        doc = {
            'alarm_type': 'multi-site high delay',
            'site': site,
            'src_sites': src_sites,
            'dest_sites': dest_sites,
            'avg_severity_multiplier': round(avg_severity, 2),
            'max_delay_p95': round(max_delay, 2),
            'total_affected_pairs': len(site_issues),
            'alarm_id': hashlib.sha224(to_hash.encode('utf-8')).hexdigest(),
            'from': convert_to_iso_format(anomalous_df.iloc[0]['from']),
            'to': convert_to_iso_format(anomalous_df.iloc[0]['to']),
            'body': f'Site {site} shows high delay to/from {len(site_issues)} destinations',
            'tags': [site]
        }
        
        if test_mode:
            all_alarm_data.append(doc)
            print(f"📊 Multi-site issue: {site}")
            print(f"   • Affected pairs: {len(site_issues)}")
            print(f"   • Max delay: {max_delay:.1f}ms")
            print(f"   • Avg severity: {avg_severity:.1f}x")
        else:
            alarm_multi.addAlarm(
                body=doc['body'],
                tags=doc['tags'],
                source=doc
            )
    
    # Handle individual pair issues (excluding multi-site ones)
    individual_issues = anomalous_df[
        ~((anomalous_df['src_site'].isin(multi_site_issues)) |
          (anomalous_df['dest_site'].isin(multi_site_issues)))
    ]

    for _, row in individual_issues.iterrows():
        to_hash = ','.join([row['src_site'], row['dest_site'], str(row['to'])])

        doc = {
            'alarm_type': 'individual high delay',
            'src_site': row['src_site'],
            'dest_site': row['dest_site'],
            'src_host': row['src_host'],
            'dest_host': row['dest_host'],
            'current_delay_p95': row['current_delay_p95'],
            'baseline_p95': row['baseline_p95'],
            'threshold': row['threshold'],
            'severity_multiplier': row['severity_multiplier'],
            'alarm_id': hashlib.sha224(to_hash.encode('utf-8')).hexdigest(),
            'from': convert_to_iso_format(row['from']),
            'to': convert_to_iso_format(row['to']),
            'body': f'High delay detected: {row["current_delay_p95"]}ms (baseline: {row["baseline_p95"]}ms)',
            'tags': [row['src_site'], row['dest_site']]
        }
        
        if test_mode:
            all_alarm_data.append(doc)
            print(f"🔗 Individual issue: {row['src_site']} -> {row['dest_site']}")
            print(f"   • Current: {row['current_delay_p95']}ms")
            print(f"   • Baseline: {row['baseline_p95']}ms")
            print(f"   • Severity: {row['severity_multiplier']}x")
        else:
            alarm_pair.addAlarm(
                body=doc['body'],
                tags=doc['tags'],
                source=doc
            )
    
    # Return dataframe in test mode
    if test_mode:
        if all_alarm_data:
            alarm_df = pd.DataFrame(all_alarm_data)
            print(f"\n📋 ALARM DATA SUMMARY:")
            print(f"Total alarms that would be sent: {len(alarm_df)}")
            return alarm_df
        else:
            print("\n📋 No alarms would be sent")
            return pd.DataFrame()


def parse_datetime_string(dt_str):
    """Parse a datetime string in either ISO 8601 or '%Y-%m-%d %H:%M:%S.%f' format."""
    from datetime import datetime
    for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M']:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    raise ValueError(f"Could not parse datetime string: {dt_str}")


if __name__ == "__main__":
    test_mode = False
    max_workers = 10
    
    # Use UTC time range (database is in UTC)
    try:
        date_from, date_to = hp.defaultTimeRange(hours=24)
        print(f"Using hp.defaultTimeRange: {date_from} - {date_to}")
    except:
        # Fallback: create UTC time range manually
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(hours=24)
        date_from = start_utc.strftime('%Y-%m-%d %H:%M')
        date_to = now_utc.strftime('%Y-%m-%d %H:%M')
        print(f"Using manual UTC time range: {date_from} - {date_to}")
    
    print(f"🚀 Detecting high OWD anomalies: {date_from} - {date_to}")
    print(f"⚡ Using {max_workers} parallel workers for baseline queries")
    if test_mode:
        print("🧪 Running in TEST MODE - no alarms will be sent")
    
    # Parse date strings to expected format for downstream functions
    parsed_date_from = parse_datetime_string(date_from).strftime('%Y-%m-%d %H:%M:%S.%f')
    parsed_date_to = parse_datetime_string(date_to).strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Detect anomalies using adaptive baselines with parallelization
    anomalous_delays = detect_high_owd_with_baselines(parsed_date_from, parsed_date_to, max_workers=max_workers)
    
    # Display results dataframe
    if not anomalous_delays.empty:
        print(f"\n📊 RESULTS DATAFRAME ({len(anomalous_delays)} anomalies):")
        
        # Show summary by delay type used
        if 'delay_type_used' in anomalous_delays.columns:
            delay_type_summary = anomalous_delays['delay_type_used'].value_counts()
            print(f"   Delay metrics used: {dict(delay_type_summary)}")
            
        if 'has_negative_mean' in anomalous_delays.columns:
            negative_mean_count = anomalous_delays['has_negative_mean'].sum()
            print(f"   Anomalies with negative mean delays: {negative_mean_count}")
        
        # Show key columns
        display_cols = ['src_site', 'dest_site', 'current_delay_p95', 'baseline_p95', 'severity_multiplier']
        if 'delay_type_used' in anomalous_delays.columns:
            display_cols.append('delay_type_used')
        
        print(anomalous_delays[display_cols].to_string())
    
    # Send alarms (or display in test mode)
    alarm_data = send_high_owd_alarms(anomalous_delays, test_mode=test_mode)
    
    print(f"\nHigh OWD detection completed. Found {len(anomalous_delays)} anomalies.")
    
    # Return data in test mode for notebook usage
    if test_mode:
        # Make results available globally for notebook access
        globals()['anomalous_delays_df'] = anomalous_delays
        globals()['alarm_data_df'] = alarm_data if alarm_data is not None else pd.DataFrame()
        
        print("\n📊 Data available in notebook as:")
        print("  • anomalous_delays_df - Raw anomalies detected")
        print("  • alarm_data_df - Alarm data that would be sent to database")





# If we want to run the detection for the past 7 days

# import os
# from datetime import datetime, timedelta
# from ps_high_owd import detect_high_owd_with_baselines, send_high_owd_alarms

# # Run for past 7 days
# end_date = datetime.now()
# for i in range(7):
#     day_end = end_date - timedelta(days=i)
#     day_start = day_end - timedelta(days=1)

#     print(f"\n🚀 Processing day {i+1}/7: {day_start.strftime('%Y-%m-%d')} to {day_end.strftime('%Y-%m-%d')}")

#     # Set date range manually
#     date_from = day_start.strftime('%Y-%m-%d %H:%M')
#     date_to = day_end.strftime('%Y-%m-%d %H:%M')

#     print(f"🚀 Detecting high OWD anomalies: {date_from} - {date_to}")

#     # Run the detection with the specific date range
#     anomalous_delays = detect_high_owd_with_baselines(date_from, date_to)

#     # Send alarms (not in test mode)
#     if not anomalous_delays.empty:
#         print(f"Found {len(anomalous_delays)} anomalies for {day_start.strftime('%Y-%m-%d')}")
#         send_high_owd_alarms(anomalous_delays, test_mode=False)
#     else:
#         print(f"No anomalies found for {day_start.strftime('%Y-%m-%d')}")


# delete_query = {
#     "query": {
#         "terms": {
#             "event.keyword": ["high one-way delay", "high delay from/to multiple sites"]
#         }
#     }
# }

# # Uncomment to actually delete:
# hp.es.delete_by_query(index="aaas_alarms", body=delete_query)