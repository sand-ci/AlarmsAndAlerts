from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
import traceback
import urllib3
import re
from geopy.geocoders import Nominatim

import utils.queries as qrs
import utils.helpers as hp


class MetaData(object):

    def __init__(self, dateFrom=None,  dateTo=None):

        fmt = "%Y-%m-%d %H:%M"
        now = hp.roundTime(datetime.utcnow())
        dateTo = datetime.strftime(now, fmt)
        # Around 1 Sept, the netsites were introduced to ES
        beforeChange = self.getEndpoints( ['2023-01-01 00:15', '2023-09-01 00:15'], False)
        afterChange = self.getEndpoints(['2023-09-01 00:15', dateTo], True)

        endpointsDf = pd.merge(afterChange[['ip', '_host', '_site' ,'ipv6', 'netsite']],
                               beforeChange[['ip', '_host', '_site' ,'ipv6']], on=['ip', 'ipv6'], how="outer", suffixes=('_after', '_before'))

        endpointsDf.loc[:, 'ip'] = endpointsDf['ip'].str.upper()
        endpointsDf = endpointsDf[(~endpointsDf['ip'].isnull()) & ~endpointsDf['ip'].isin(['%{[DESTINATION][IPV4]}', '%{[DESTINATION][IPV6]}'])]

        endpointsDf['site'] = endpointsDf.apply(lambda row: self.combine_sites(row), axis=1)
        endpointsDf['netsite'] = endpointsDf['netsite'].fillna(endpointsDf['site'])
        endpointsDf['host'] = endpointsDf['_host_after'].fillna(endpointsDf['_host_before'])

        endpointsDf = self.fixUnknownSites(endpointsDf)
        endpointsDf = endpointsDf[['ip', 'site', 'ipv6', 'host', 'netsite']].drop_duplicates()


        metaDf = self.getMeta(endpointsDf)
        metaDf = self.fixUnknownWithNetsite(metaDf)

        # metaDf.loc[:, 'ip_original'] = metaDf['ip']
        metaDf.loc[:, 'site_original'] = metaDf['site']
        metaDf.loc[:, 'netsite_original'] = metaDf['netsite']
        metaDf.loc[:, 'site_meta'] = metaDf['site_meta']

        metaDf.loc[:, 'ip'] = metaDf['ip'].str.upper()
        metaDf.loc[:, 'site'] = metaDf['site'].str.upper()
        metaDf.loc[:, 'netsite'] = metaDf['netsite'].str.upper()
        metaDf.loc[:, 'site_meta'] = metaDf['site_meta'].str.upper()

        # fill in empty lat and lon based on host, ip, site
        metaDf = metaDf.sort_values(['lat', 'lon'])
        metaDf = metaDf.fillna(metaDf[['host', 'lat', 'lon']].groupby('host').ffill())
        metaDf = metaDf.fillna(metaDf[['ip', 'lat', 'lon']].groupby('ip').ffill())
        metaDf = metaDf.fillna(metaDf[['site', 'lat', 'lon']].groupby('site').ffill())
        metaDf = metaDf.fillna(metaDf[['site_meta', 'lat', 'lon']].groupby('site_meta').ffill())

        # mdf_sorted = metaDf.sort_values(by=['ip', 'site', 'netsite'])
        # # when an ip has a few records, prefer one where site != netsite
        # metaDf = mdf_sorted.drop_duplicates(subset='ip', keep='last')

        metaDf = self.locateCountry(metaDf)

        metaDf = metaDf.drop_duplicates()

        print(f"Endpoints without a location: {len(metaDf[metaDf['lat'].isnull()])}")

        self.metaDf = metaDf


    @staticmethod
    def locateCountry(df):
        geolocator = Nominatim(user_agent="my_app")

        def get_country(lat, lon):
          location = geolocator.reverse((lat, lon), exactly_one=True, language='en')
          return location.raw['address']['country']

        unique_lat_lon = df[['lat', 'lon']].drop_duplicates()
        unique_lat_lon['country'] = unique_lat_lon.apply(lambda row: get_country(row['lat'], row['lon']), axis=1)
        lat_lon_to_country = dict(zip(unique_lat_lon.set_index(['lat', 'lon']).index, unique_lat_lon['country']))

        df['country'] = df.apply(lambda row: lat_lon_to_country.get((row['lat'], row['lon'])), axis=1)

        return df


    @staticmethod
    def queryEndpoints(dt, endpoint, idx, netsiteExists):
        data = []
        query = {
            "bool": {
              "must": [
                {
                  "range": {
                    "timestamp": {
                      "format": "yyyy-MM-dd HH:mm",
                      "gte": dt[0],
                      "lte": dt[1]
                    }
                  }
                }
                ]
            }
          }


        aggregations = {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    "ipv6" : {
                      "terms" : {
                        "field" : "ipv6"
                      }
                    }
                  },
                  {
                    endpoint : {
                      "terms" : {
                        "field" : endpoint
                      }
                    }
                  },
                  {
                    f"{endpoint}_host" : {
                      "terms" : {
                        "field" : f"{endpoint}_host"
                      }
                    }
                  },
                  {
                    f"{endpoint}_site" : {
                      "terms" : {
                        "field" : f"{endpoint}_site"
                      }
                    }
                  }
                ]
              }
            }
          }

        if netsiteExists:
            # print(aggregations['groupby']['composite']['sources'])
            aggregations['groupby']['composite']['sources'].append({
                                                                    f"{endpoint}_netsite" : {
                                                                      "terms" : {
                                                                        "field" : f"{endpoint}_netsite"
                                                                      }
                                                                    }
                                                                  })

        # print(str(query).replace("\'", "\""))
        # print(str(aggregations).replace("\'", "\""))
        data = []
        aggdata = hp.es.search(index=idx, query=query, aggregations=aggregations, size=0)

        for item in aggdata['aggregations']['groupby']['buckets']:
            ip = item['key'][endpoint]

            row = { 'ip': ip, 'ipv6': item['key']['ipv6'],
                  '_host': item['key'][f'{endpoint}_host'],
                  '_site': item['key'][f'{endpoint}_site'],
                 }
            if f'{endpoint}_netsite' in item['key']:
                row['netsite'] = item['key'][f'{endpoint}_netsite']
            data.append(row)

        df = pd.DataFrame(data)

        # unique_combinations = df[['ip', ':site']].groupby(['ip']).nunique().sort_values(':site').\
        #                                                          rename(columns={':site':'count'}).reset_index()
        # moreSites = unique_combinations[unique_combinations['count'] > 1]['ip'].tolist()

        # if len(moreSites) > 1:
        #     print("The following IP addressed have multiple site names:")
        #     for ip in moreSites:
        #         display(df[df['ip']==ip])

        return df


    def getEndpoints(self, dt, netsiteExists):
        srcdf = self.queryEndpoints(dt, 'src', 'ps_packetloss', netsiteExists)
        destdf = self.queryEndpoints(dt, 'dest', 'ps_packetloss', netsiteExists)
        mergeddf = pd.merge(srcdf, destdf, how='outer')

        srcdf = self.queryEndpoints(dt, 'src', 'ps_owd', netsiteExists)
        destdf = self.queryEndpoints(dt, 'dest', 'ps_owd', netsiteExists)
        mergeddf1 = pd.merge(srcdf, destdf, how='outer')
        mergeddf = pd.merge(mergeddf, mergeddf1, how='outer')

        srcdf = self.queryEndpoints(dt, 'src', 'ps_throughput', netsiteExists)
        destdf = self.queryEndpoints(dt, 'dest', 'ps_throughput', netsiteExists)
        mergeddf1 = pd.merge(srcdf, destdf, how='outer')
        mergeddf = pd.merge(mergeddf, mergeddf1, how='outer')

        srcdf = self.queryEndpoints(dt, 'src', 'ps_trace', netsiteExists)
        destdf = self.queryEndpoints(dt, 'dest', 'ps_trace', netsiteExists)
        mergeddf1 = pd.merge(srcdf, destdf, how='outer')
        mergeddf = pd.merge(mergeddf, mergeddf1, how='outer')

        return mergeddf


    @staticmethod
    def combine_sites(row):
        if pd.isna(row['_site_after']) or row['_site_after'] == '' or row['_site_after'] == 'UNKNOWN':
            if not pd.isna(row['_site_before']) and row['_site_before'] != '' and row['_site_before'] != 'UNKNOWN':
                return row['_site_before']

        return row['_site_after']


    @staticmethod
    def flatten_extend(lists):
        flat_list = []
        for row in lists:
            flat_list.extend(row)
        return set(flat_list)


    def fixUnknownSites(self, endpointsDf):
        for idx, row in endpointsDf[endpointsDf['site']=='UNKNOWN'][['ip', 'host']].iterrows():
            ip, host = row
            # print(idx)
            values = endpointsDf[((endpointsDf['ip']==ip) | (endpointsDf['host']==host)) &
                                (endpointsDf['site']!='UNKNOWN')][['_site_before', '_site_after']].drop_duplicates().values.tolist()
            possibility = [x for x in self.flatten_extend(values) if x==x and x!='UNKNOWN']
            if len(possibility)==1:
                endpointsDf.loc[endpointsDf['ip'] == ip, 'site'] = possibility[0]
            # else:
            #     print(possibility)
        return endpointsDf


    @staticmethod
    def __isHost(val):
        if re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val):
            return True
        return False


    def mostRecentMetaRecord(self, ip, site, host, ipv6, netsite):
        ipv = 'ipv6' if ipv6 == True else 'ipv4'
        site = netsite if site.upper() == 'UNKNOWN' else site

        q_ip = {
            "bool" : {
              "must" : [
                {
                  "term" : {
                    f"external_address.{ipv}_address" : {
                      "value" : ip,
                      "case_insensitive": True
                    }
                  }
                },
                {
                  "exists": {
                    "field": "geolocation"
                  }
                }
              ]
            }
          }


        q_site = {
            "bool": {
              "must":
                [
                    {
                    "bool": {
                      "should": [
                        {
                          "term": {
                            "site.keyword": {
                              "value": site,
                              "case_insensitive": True
                            }
                          }
                        },
                        {
                          "term": {
                            "config.site_name.keyword": {
                              "value": site,
                              "case_insensitive": True
                            }
                          }
                        },
                        {
                          "term": {
                            "rcsite.keyword": {
                              "value": site,
                              "case_insensitive": True
                            }
                          }
                        }
                      ]
                    }
                  },
                 {
                  "exists": {
                    "field": "geolocation"
                  }
                }
                ]
            }
        }

        q_host = {
            "bool" : {
              "must" : [
                {
                  "term" : {
                    "host" : {
                      "value" : host,
                      "case_insensitive": True
                    }
                  }
                },
                {
                  "exists": {
                    "field": "geolocation"
                  }
                }
              ]
            }
          }


        # print(str(q).replace("\'", "\""))
        doc = {}
        not_in = []
        sort = {"timestamp" : {"order" : "desc"}}
        source = ["geolocation",f"external_address.{ipv}_address", "config.site_name",
                  "host","administrator.name","administrator.email","timestamp"]



        data = hp.es.search(index='ps_meta', query=q_ip, size=1, _source=source, sort=sort)

        if data['hits']['hits']:
            records = data['hits']['hits'][0]['_source']
        else:
            if site and site==site:
                data = hp.es.search(index='ps_meta', query=q_site, size=1, _source=source, sort=sort)
            if not data['hits']['hits']:
                if self.__isHost(host):
                    data = hp.es.search(index='ps_meta', query=q_host, size=1, _source=source, sort=sort)



        if len(data['hits']['hits'])>0:
            records = data['hits']['hits'][0]['_source']
            doc['ip'] = ip
            doc['site'] = site
            doc['netsite'] = netsite
            doc['host'] = host
            doc['ipv6'] = ipv6
            if 'timestamp' in records:
                doc['timestamp'] = records['timestamp']
            if 'host' in records:
                doc['host'] = records['host']
            if 'config' in records:
                if 'site_name' in records['config']:
                    doc['site_meta'] = records['config']['site_name']
            if 'administrator' in records:
                if 'name' in records['administrator']:
                    doc['administrator'] = records['administrator']['name']
                if 'email' in records['administrator']:
                    doc['email'] = records['administrator']['email']
            if 'geolocation' in records:
                doc['lat'], doc['lon'] = records['geolocation'].split(",")
        else:
            not_in = [ip, site, host, ipv6]

        return doc, not_in


    # site, ip, ipv6, host = endpointsDf[['site', 'ip', 'ipv6', '_host_before']].values.tolist()[-12]
    # ip, ipv6, host, site = '2001:638:700:10a8:0:0:1:1e', True, 'perfsonar-ps-02.desy.de', 'DESY-HH'
    # rec = mostRecentMetaRecord(ip, site, host, ipv6)
    def getMeta(self, endpointsDf):
        data = []
        not_in_meta, uk = [],[]
        for ip, site, ipv6, host, netsite in endpointsDf.values.tolist():
            rec, not_in = self.mostRecentMetaRecord(ip, site, host, ipv6, netsite)
            if site == 'UNKNOWN': 
                uk.append(ip)
                # print(ip, site, ipv6, host)
            if rec:
                data.append(rec)
            elif not_in:
                not_in_meta.append(not_in)

        # notindf = pd.DataFrame(not_in_meta).tocsv
        # notindf

        df = pd.DataFrame(data)
        df.sort_values('timestamp', ascending=False, inplace=True)

        # Drop duplicates, keeping the first occurrence (which, after sorting, is the most recent date)
        df.drop_duplicates(subset='ip', keep='first', inplace=True)

        return df


    def fixUnknownWithNetsite(self, df):
        for idx, row in df[df['site']=='UNKNOWN'][['ip', 'host']].iterrows():
            ip, host = row
            values = df[((df['ip']==ip) | (df['host']==host))][['netsite', 'site_meta']].drop_duplicates().values.tolist()
            possibility = [x for x in self.flatten_extend(values) if x==x and x!='UNKNOWN']
            # print(ip, host, possibility, values)
            if len(possibility)>=1:
                df.loc[df['ip'] == ip, 'site'] = possibility[0]
            # else:
            #     print(possibility)
        return df