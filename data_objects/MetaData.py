from elasticsearch.helpers import scan
from datetime import datetime, timedelta, timezone
import pandas as pd
import re
from geopy.geocoders import Nominatim
import requests

import utils.helpers as hp
import ssl
import urllib3
import time
from elasticsearch.exceptions import ConnectionTimeout, TransportError
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MetaData(object):

    def __init__(self, dateFrom=None,  dateTo=None):

      current_date = datetime.now(timezone.utc)
      dateTo = current_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
      dateFrom = (current_date- timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

      endpointsDf = self.getEndpoints([dateFrom, dateTo], True)

      endpointsDf.loc[:, 'ip'] = endpointsDf['ip'].str.upper()
      endpointsDf = endpointsDf[(~endpointsDf['ip'].isnull()) & ~endpointsDf['ip'].isin(['%{[DESTINATION][IPV4]}', '%{[DESTINATION][IPV6]}'])]

      endpointsDf['site'] = endpointsDf['netsite']
      endpointsDf['netsite'] = endpointsDf['netsite'].fillna(endpointsDf['_site'])
      endpointsDf['host'] = endpointsDf['_host']

      endpointsDf = self.fixUnknownSites(endpointsDf)
      endpointsDf = endpointsDf[['ip', 'site', 'ipv6', 'host', 'netsite']].drop_duplicates()

      metaDf = self.getMeta(endpointsDf)
      metaDf = self.fixUnknownWithNetsite(metaDf)

      # metaDf.loc[:, 'site_original'] = metaDf['site']
      # metaDf.loc[:, 'netsite_original'] = metaDf['netsite']
      metaDf.loc[:, 'site_meta'] = metaDf['site_meta']

      metaDf.loc[:, 'ip'] = metaDf['ip'].str.upper()
      metaDf.loc[:, 'site'] = metaDf['site'].str.upper()
      metaDf.loc[:, 'netsite'] = metaDf['netsite'].str.upper()
      metaDf.loc[:, 'site_meta'] = metaDf['site_meta'].str.upper()

      metaDf = metaDf.sort_values(['lat', 'lon'])
      metaDf = metaDf.fillna(metaDf[['host', 'lat', 'lon']].groupby('host').ffill())
      metaDf = metaDf.fillna(metaDf[['ip', 'lat', 'lon']].groupby('ip').ffill())
      metaDf = metaDf.fillna(metaDf[['site', 'lat', 'lon']].groupby('site').ffill())
      metaDf = metaDf.fillna(metaDf[['site_meta', 'lat', 'lon']].groupby('site_meta').ffill())

      metaDf = self.locateCountry(metaDf)

      metaDf = metaDf.drop_duplicates()

      print(f"Endpoints without a location: {len(metaDf[metaDf['lat'].isnull()])}")

      self.metaDf = metaDf


    @staticmethod
    def locateCountry(df):
        # Create SSL context that bypasses certificate verification        
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        geolocator = Nominatim(
            user_agent="my_app",
            ssl_context=ctx,
            timeout=15
        )

        def get_country_backup(lat, lon):
            """Backup method using REST Countries API via coordinates"""
            try:
                # Use a simple REST API that doesn't require SSL
                url = f"http://api.geonames.org/countryCodeJSON?lat={lat}&lng={lon}&username=demo"
                response = requests.get(url, timeout=10, verify=False)
                if response.status_code == 200:
                    data = response.json()
                    if 'countryName' in data:
                        return data['countryName']
                        
            except Exception as e:
                print(f"Backup geocoding also failed for {lat}, {lon}: {e}")
            
            return None

        def get_country(lat, lon):
            try:
                # Try Nominatim first
                for attempt in range(2):  # Reduced attempts for primary method
                    try:
                        location = geolocator.reverse(
                            (lat, lon), 
                            exactly_one=True, 
                            language='en', 
                            timeout=10
                        )
                        if location and hasattr(location, 'raw') and 'address' in location.raw and 'country' in location.raw['address']:
                            return location.raw['address']['country']
                    except Exception as e:
                        if attempt == 1:  # Last attempt with Nominatim
                            print(f"Nominatim failed for {lat}, {lon}: {e}")
                            break
                        time.sleep(1)
                
                # Try backup method
                print(f"Trying backup geocoding for {lat}, {lon}")
                return get_country_backup(lat, lon)
                
            except Exception as e:
                print(f"All geocoding methods failed for {lat}, {lon}: {e}")
                return None

        unique_lat_lon = df[['lat', 'lon']].dropna().drop_duplicates()
        unique_lat_lon['country'] = unique_lat_lon.apply(lambda row: get_country(row['lat'], row['lon']), axis=1)
        lat_lon_to_country = dict(zip(unique_lat_lon.set_index(['lat', 'lon']).index, unique_lat_lon['country']))

        df['country'] = df.apply(lambda row: lat_lon_to_country.get((row['lat'], row['lon'])) if pd.notna(row['lat']) and pd.notna(row['lon']) else None, axis=1)

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
                      "gte": dt[0],
                      "lte": dt[1],
                      "format": "strict_date_optional_time"
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
        
        # Add retry logic for Elasticsearch queries
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                aggdata = hp.es.search(
                    index=idx, 
                    query=query, 
                    aggregations=aggregations, 
                    size=0,
                    timeout='30s',
                    request_timeout=30
                )
                break
            except (ConnectionTimeout, TransportError) as e:
                if attempt == max_retries - 1:
                    print(f"Failed to query {idx} {endpoint} after {max_retries} attempts: {e}")
                    return pd.DataFrame()  # Return empty DataFrame on final failure
                print(f"Query attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

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
        if re.match(r"^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val):
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
        source = ["geolocation", f"external_address.{ipv}_address", "config.site_name",
                  "host", "administrator.name", "administrator.email", "timestamp"]
        
        # Add timeout and retry for meta searches
        def search_with_retry(query, description):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    return hp.es.search(
                        index='ps_meta', 
                        query=query, 
                        size=1, 
                        _source=source, 
                        sort=sort,
                        timeout='15s',
                        request_timeout=15
                    )
                except (ConnectionTimeout, TransportError) as e:
                    if attempt == max_retries - 1:
                        print(f"Failed {description} search after {max_retries} attempts: {e}")
                        return {'hits': {'hits': []}}
                    time.sleep(2 ** attempt)  # Exponential backoff
            return {'hits': {'hits': []}}

        data = search_with_retry(q_ip, "IP")

        if data['hits']['hits']:
            records = data['hits']['hits'][0]['_source']
        else:
            if site and site==site:
                data = search_with_retry(q_site, "site")
            if not data['hits']['hits']:
                if self.__isHost(host):
                    data = search_with_retry(q_host, "host")

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