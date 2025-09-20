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

      # Only call locateCountry once and check if country column already exists
      if 'country' not in metaDf.columns:
          metaDf = self.locateCountry(metaDf)

      metaDf = metaDf.drop_duplicates()

      print(f"Endpoints without a location: {len(metaDf[metaDf['lat'].isnull()])}")

      # Store the final processed dataframe
      self.metaDf = metaDf
    
    def get_dataframe(self):
        """Return the processed metadata dataframe"""
        return self.metaDf.copy()  # Return a copy to prevent external modification
    
    @property
    def dataframe(self):
        """Property to access the dataframe"""
        return self.metaDf


    @staticmethod
    def locateCountry(df):        
        def get_country_backup(lat, lon):
            try:
                # Simple coordinate-to-country mapping for major regions
                lat, lon = float(lat), float(lon)
                
                # Define country boundaries (simplified)
                country_regions = {
                    # North America
                    "United States": [(25, 50), (-125, -65)],
                    "Canada": [(45, 85), (-140, -50)],
                    "Mexico": [(14, 33), (-118, -86)],
                    
                    # Europe
                    "Germany": [(47, 55), (5, 16)],
                    "France": [(42, 51), (-5, 8)],
                    "United Kingdom": [(49, 61), (-8, 2)],
                    "Italy": [(36, 47), (6, 19)],
                    "Spain": [(35, 44), (-10, 5)],
                    "Netherlands": [(50, 54), (3, 8)],
                    "Switzerland": [(45, 48), (5, 11)],
                    "Poland": [(49, 55), (14, 25)],
                    "Czech Republic": [(48, 51), (12, 19)],
                    
                    # Asia
                    "Japan": [(24, 46), (123, 146)],
                    "China": [(18, 54), (73, 135)],
                    "India": [(6, 37), (68, 97)],
                    "South Korea": [(33, 39), (124, 132)],
                    
                    # South America
                    "Brazil": [(-34, 6), (-74, -32)],
                    "Argentina": [(-55, -21), (-74, -53)],
                    "Chile": [(-56, -17), (-76, -66)],
                    
                    # Australia/Oceania
                    "Australia": [(-44, -10), (113, 154)],
                    "New Zealand": [(-47, -34), (166, 179)],
                    
                    # Africa
                    "South Africa": [(-35, -22), (16, 33)],
                    "Egypt": [(22, 32), (25, 35)],
                }
                
                # Check which country the coordinates fall into
                for country, ((min_lat, max_lat), (min_lon, max_lon)) in country_regions.items():
                    if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                        return country
                        
                # Fallback based on continent
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    if 35 <= lat <= 71 and -10 <= lon <= 70:
                        return "Europe"
                    elif 5 <= lat <= 55 and 60 <= lon <= 150:
                        return "Asia"
                    elif 15 <= lat <= 83 and -170 <= lon <= -50:
                        return "North America"
                    elif -60 <= lat <= 15 and -85 <= lon <= -30:
                        return "South America"
                    elif -50 <= lat <= 40 and -20 <= lon <= 60:
                        return "Africa"
                    elif -50 <= lat <= -10 and 110 <= lon <= 180:
                        return "Australia"
                        
            except Exception as e:
                print(f"Coordinate mapping failed for {lat}, {lon}: {e}")
            
            return "Unknown"

        def get_country_from_ip_or_host(df_row):
            """Extract country from hostname or use coordinate mapping"""
            try:
                # Try to extract country from hostname
                host = df_row.get('host', '')
                if host and isinstance(host, str):
                    host_lower = host.lower()
                    
                    # Special patterns for specific organizations
                    if 'ndgf.org' in host_lower:
                        return 'Sweden'
                    
                    # Common country codes in hostnames
                    country_codes = {
                        '.us': 'United States', '.edu': 'United States',
                        '.uk': 'United Kingdom', '.ac.uk': 'United Kingdom',
                        '.de': 'Germany', '.fr': 'France', '.it': 'Italy',
                        '.nl': 'Netherlands', '.ch': 'Switzerland',
                        '.jp': 'Japan', '.cn': 'China', '.au': 'Australia',
                        '.br': 'Brazil', '.ca': 'Canada', '.mx': 'Mexico',
                        '.pl': 'Poland', '.cz': 'Czech Republic',
                        '.es': 'Spain', '.se': 'Sweden', '.no': 'Norway',
                        '.dk': 'Denmark', '.fi': 'Finland', '.be': 'Belgium',
                        '.at': 'Austria', '.pt': 'Portugal', '.gr': 'Greece',
                        '.hu': 'Hungary', '.ro': 'Romania', '.bg': 'Bulgaria',
                        '.hr': 'Croatia', '.si': 'Slovenia', '.sk': 'Slovakia',
                        '.ie': 'Ireland', '.is': 'Iceland', '.lu': 'Luxembourg',
                        '.ua': 'Ukraine', '.ru': 'Russia', '.su': 'Russia',
                    }
                    
                    for code, country in country_codes.items():
                        if code in host_lower:
                            return country
                
                # If hostname doesn't help, use coordinates
                lat, lon = df_row.get('lat'), df_row.get('lon')
                if pd.notna(lat) and pd.notna(lon):
                    return get_country_backup(lat, lon)
                    
            except Exception as e:
                print(f"Country extraction failed: {e}")
            
            return "Unknown"

        df['country'] = df.apply(get_country_from_ip_or_host, axis=1)
        
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