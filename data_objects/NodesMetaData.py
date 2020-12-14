from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import socket
import re
from ipwhois import IPWhois
import difflib

import utils.queries as qrs
import utils.helpers as hp
from utils.helpers import timer


class NodesMetaData:

    def __init__(self, index, dateFrom,  dateTo):
        self.idx = index
        ts = hp.GetTimeRanges(dateFrom, dateTo)
        self.dateFrom, self.dateTo = ts[0], ts[1]
        self.df = self.BuildDataFrame()


    def resolveHost(self, host):
        h = ''
        try:
            if host == '127.0.0.1':
                print("Incorrect value for host")
            h = socket.gethostbyaddr(host)[0]
        except Exception as inst:
            print(str("socket exception: "+inst.args[1]))
            h = host
        return h


    def isHost(self, val):
        return re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val)


    def findHost(self, row, df):
        host_from_index = row['host_index']
        host_from_meta = row['host_meta']

        if self.isHost(host_from_index):
            return host_from_index
        else:
            # if it's an IP, try to get the hostname from a ps_meta records
            if (host_from_meta is not np.nan):
                if self.isHost(host_from_meta):
                    return host_from_meta
            # it is possible that for that IP has another record with the valid hostname
            elif len(df[df['ip'] == host_from_index]['host_index'].values) > 0:
                return df[df['ip'] == host_from_index]['host_index'].values[0]
            # otherwise we try to resolve the IP from socket.gethostbyaddr(host)
            else: 
                return self.resolveHost(host_from_index)
        return ''


    def findSite(self, row, df, hosts):
        # we tend to fill in only sites for hosts part of the configuration
        if ((row['site_x'] is None) and (row['host_in_ps_meta'] == True)):
            ratio = {}
            hs = row['host']
            meta_sites = df[(df['host'] == hs)]['site_y'].values
            first_non_null = next(filter(bool, (x for x in meta_sites)), None)
            # if site name is available in ps_meta data, take it
            # first_non_null == first_non_null - that expression checks if the value is NaN
            if ((first_non_null) and (first_non_null == first_non_null)):
                return first_non_null
            else:
                # otherwise find a similar host name and take its site name
                for h in hosts:
                    if h != hs:
                        similarity = difflib.SequenceMatcher(None, hs, h).ratio()
                        ratio[h] = similarity
                # get the value with the highest similarity score
                sib =  max(ratio, key=ratio.get)

                sib_site_index = df[df['host'] == sib]['site_x'].values
                # get the first non-null from the index
                fnn_index = next(filter(bool, (x for x in sib_site_index)), None)
                sib_site_meta = df[df['host'] == sib]['site_y'].values
                fnn_meta = next(filter(bool, (x for x in sib_site_meta)), None)

                # Check for the site name of the sibling
                if (fnn_index):
                    return fnn_index
                elif (fnn_meta):
                    return fnn_meta
                # otherwise get get IPWhoIs network name
                else:
                    return self.getIPWhoIs(sib)
        else: 
            return row['site_x']


    def getIPWhoIs(self, item):
        val = ''
        try:
            obj = IPWhois(item)
            res = obj.lookup_whois()
            val = res['nets'][0]['name']
        except Exception as inst:
            if self.isHost(item):
                val = ''
            else: val = inst.args
        return val


    @timer
    def BuildDataFrame(self):
        print('Query ', self.idx, ' for the period', self.dateFrom, '-', self.dateTo)
        
        # get metadata
        meta_df = pd.DataFrame.from_dict(qrs.get_metadata(self.dateFrom, self.dateTo), orient='index',
                                 columns=['site', 'admin_name', 'admin_email', 'ipv6', 'ipv4']).reset_index().rename(columns={'index': 'host'})


        # in ES there is a limit of up to 10K buckets for aggregation, 
        # thus we need to split the queries and then merge the results
        ip_site_df = pd.DataFrame.from_dict(qrs.get_ip_site(self.idx, self.dateFrom, self.dateTo),
                                            orient='index', columns=['site', 'is_ipv6']).reset_index().rename(columns={'index': 'ip'})
        ip_host_df = pd.DataFrame.from_dict(qrs.get_ip_host(self.idx, self.dateFrom, self.dateTo),
                                            orient='index', columns=['host']).reset_index().rename(columns={'index': 'ip'})
        host_site_df = pd.DataFrame.from_dict(qrs.get_host_site(self.idx, self.dateFrom, self.dateTo),
                                              orient='index', columns=['site']).reset_index().rename(columns={'index': 'host'})

        df = pd.merge(ip_site_df, ip_host_df, how='outer', left_on=['ip'], right_on=['ip'])
        df = pd.merge(df, host_site_df, how='left', left_on=['host', 'site'], right_on=['host', 'site'])
        df = pd.merge(df, meta_df[['host', 'site']], how='left', left_on=['host'], right_on=['host'])

        df = pd.merge(df, meta_df[['host', 'ipv4']], how='left', left_on=['ip'], right_on=['ipv4'])
        df = pd.merge(df, meta_df[['host', 'ipv6']], how='left', left_on=['ip'], right_on=['ipv6'])

        df['host_meta'] = np.where((df['host_y'].isnull()), df['host'], df['host_y'])
        df.drop(['host_y', 'host'], inplace=True, axis=1)
        df = pd.merge(df, meta_df[['host', 'admin_email', 'admin_name']], how='left', left_on=['host_meta'], right_on=['host'])

        # mark hosts and IPs not part of the configuration
        df['ip_in_ps_meta'] = np.where((df['ipv4'].isnull() & df['ipv6'].isnull()), False, True)
        df['host_in_ps_meta'] = np.where((df['host_meta'].isnull()), False, True)

        df.drop(['ipv4', 'ipv6', 'host'], inplace=True, axis=1)
        df.rename(columns={'host_x': 'host_index', 'host_y': 'host_meta'}, inplace=True)

        df['host_has_aliases'] = df.apply(lambda row: True if (row['ip'] in df.loc[df.duplicated('ip', keep=False)]['ip'].values) 
                                                           else False, axis=1)
        # # in some cases there are > 1 rows having the same IP due to slightly different site name or host name takes a CNAME
        df.drop_duplicates(subset='ip', keep="first", inplace=True)

        df['host'] = df.apply(lambda row: self.findHost(row, df), axis=1)

        # get site name if it exists in ps_*, else get site name from ps_meta if it's not null,
        # otherwise use IPWhoIS network name, but only fro the hosts part of the configuration (present in ps_meta)
        hosts = df[df['host_in_ps_meta'] == True]['host'].values
        df['site'] = df.apply(lambda row: self.findSite(row, df, hosts), axis=1)
        df.rename(columns={'site_x': 'site_index', 'site_y': 'site_meta'}, inplace=True)

        return df