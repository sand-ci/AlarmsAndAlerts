from datetime import datetime, timedelta
import dateutil.relativedelta
import time
import os
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import getpass
import functools


with open('/config/config.json') as json_data:
    config = json.load(json_data,)

es = Elasticsearch(
    hosts=[{'host': config['ES_HOST'], 'scheme':'https'}],
    http_auth=(config['ES_USER'], config['ES_PASS']),
    timeout=60)


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


'''Returns a period of the past 3 hours'''


def defaultTimeRange(hours=3):
    now = datetime.utcnow()
    defaultEnd = datetime.strftime(now, '%Y-%m-%d %H:%M')
    defaultStart = datetime.strftime(
        now - timedelta(hours=hours), '%Y-%m-%d %H:%M')

    return [defaultStart, defaultEnd]


'''Finds the difference between two dates'''


def FindPeriodDiff(dateFrom, dateTo):
    fmt = '%Y-%m-%d %H:%M'
    d1 = datetime.strptime(dateFrom, fmt)
    d2 = datetime.strptime(dateTo, fmt)
    time_delta = d2-d1

    return time_delta


'''Splits the period into chunks of specified number of intervals.'''


def GetTimeRanges(dateFrom, dateTo, intv=1):
    diff = FindPeriodDiff(dateFrom, dateTo) / intv
    t_format = "%Y-%m-%d %H:%M"
    tl = []
    for i in range(intv+1):
        t = (datetime.strptime(dateFrom, t_format) + diff * i).strftime(t_format)
        tl.append(int(time.mktime(datetime.strptime(t, t_format).timetuple())*1000))

    return tl


'''The following method helps to calculate the expected number of tests for a specific period'''


def CalcMinutes4Period(dateFrom, dateTo):
    time_delta = FindPeriodDiff(dateFrom, dateTo)

    return (time_delta.days*24*60 + time_delta.seconds//60)
