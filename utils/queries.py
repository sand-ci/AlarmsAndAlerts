from elasticsearch.helpers import scan
import utils.helpers as hp
import pandas as pd


valueField = {
            'ps_packetloss': 'packet_loss',
            'ps_owd': 'delay_mean',
            'ps_retransmits': 'retransmits',
            'ps_throughput': 'throughput'
            }


def getMetaData():
    meta = []
    data = scan(hp.es, index='ps_alarms_meta')
    for item in data:
        meta.append(item['_source'])

    if meta:
        return pd.DataFrame(meta)


def allTestedNodes(period):
    def query(direction):
        return {
          "size" : 0,
          "query" : {
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "gt" : period[0],
                      "lte": period[1]
                    }
                  }
                }
              ]
            }
          },
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    direction : {
                      "terms" : {
                        "field" : direction,
                        "missing_bucket" : True
                      }
                    }
                  },
                  {
                    f"{direction}_host" : {
                      "terms" : {
                        "field" : f"{direction}_host",
                        "missing_bucket" : True
                      }
                    }
                  },
                  {
                    f"{direction}_site" : {
                      "terms" : {
                        "field" : f"{direction}_site",
                        "missing_bucket" : True
                      }
                    }
                  },
                  {
                    "ipv6" : {
                      "terms" : {
                        "field" : "ipv6",
                        "missing_bucket" : True
                      }
                    }
                  }
                ]
              }
            }
          }
        }
    aggrs = []
    pairsDf = pd.DataFrame()
    for idx in hp.INDICES:
        aggdata = hp.es.search(index=idx, body=query('src'))
        aggrs = []
        for item in aggdata['aggregations']['groupby']['buckets']:
          site = ''
          if item['key']['src_site']:
            site = item['key']['src_site'].upper()
          aggrs.append({
                        'ip': item['key']['src'],
                        'ipv6': item['key']['ipv6'],
                        'host': item['key']['src_host'],
                        'site': site,
          })

        aggdata = hp.es.search(index=idx, body=query('dest'))
        for item in aggdata['aggregations']['groupby']['buckets']:
          site = ''
          if item['key']['dest_site']:
              site = item['key']['dest_site'].upper()
          aggrs.append({
                        'ip': item['key']['dest'],
                        'ipv6': item['key']['ipv6'],
                        'host': item['key']['dest_host'],
                        'site': site,
                        })

        # pairsDf = pairsDf.append(aggrs)
        pairsDf = pd.concat([pairsDf, pd.DataFrame(aggrs)])
        pairsDf = pairsDf.drop_duplicates()
        # print(idx, 'Len unique nodes ',len(pairsDf), 'period:', period)
    return pairsDf


def mostRecentMetaRecord(ip, ipv6, period):
    forTimeRange=''
    if period:
        forTimeRange = {
                  "range" : {
                    "timestamp" : {
                      "gt" : period[0],
                      "lte": period[1]
                    }
                  }
                }

    def q(ip, ipv):
        return {
          "size" : 1,
          "_source": ["geolocation",f"external_address.{ipv}_address", "config.site_name", "host","administrator.name","administrator.email","timestamp"],
            "sort" : [
            {
              "timestamp" : {
                "order" : "desc"
              }
            }
          ],
          "query" : {
            "bool" : {
              "must" : [
                forTimeRange,
                {
                  "term" : {
                    f"external_address.{ipv}_address" : {
                      "value" : ip
                    }
                  }
                },
                {
                  "bool": {
                    "should": [
                      {
                        "exists": {"field": "host"}
                      },
                      {
                        "exists": {"field": "config.site_name"}
                      },
                      {
                        "exists": {"field": "geolocation"}
                      },
                      {
                        "exists": {"field": "administrator.email"}
                      }
                    ]
                  }
                }
              ]
            }
          }
        }

    ipv = 'ipv6' if ipv6 == True else 'ipv4'
#     print(str(q).replace("\'", "\""))
    values = {}
    data = hp.es.search(index='ps_meta', body=q(ip,ipv))

    if data['hits']['hits']:
        records = data['hits']['hits'][0]['_source']
        values['ip'] = ip
        if 'timestamp' in records:
            values['timestamp'] = records['timestamp']
        if 'host' in records:
            values['host'] = records['host']
        if 'config' in records:
            if 'site_name' in records['config']:
                values['site_meta'] = records['config']['site_name'].upper()
            else: values['site_meta'] = ''
        else: values['site_meta'] = ''
        if 'administrator' in records:
            if 'name' in records['administrator']:
                values['administrator'] = records['administrator']['name']
            if 'email' in records['administrator']:
                values['email'] = records['administrator']['email']
        if 'geolocation' in records:
            values['lat'], values['lon'] = records['geolocation'].split(",")
    return values



def query4Avg(idx, dateFrom, dateTo):
    val_fld = valueField[idx]
    query = {
              "size" : 0,
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "range" : {
                        "timestamp" : {
                          "gt" : dateFrom,
                          "lte": dateTo
                        }
                      }
                    },
                    {
                      "term" : {
                        "src_production" : True
                      }
                    },
                    {
                      "term" : {
                        "dest_production" : True
                      }
                    }
                  ]
                }
              },
              "aggregations" : {
                "groupby" : {
                  "composite" : {
                    "size" : 9999,
                    "sources" : [
                      {
                        "src" : {
                          "terms" : {
                            "field" : "src"
                          }
                        }
                      },
                      {
                        "dest" : {
                          "terms" : {
                            "field" : "dest"
                          }
                        }
                      },
                      {
                        "src_host" : {
                          "terms" : {
                            "field" : "src_host"
                          }
                        }
                      },
                      {
                        "dest_host" : {
                          "terms" : {
                            "field" : "dest_host"
                          }
                        }
                      },
                      {
                        "src_site" : {
                          "terms" : {
                            "field" : "src_site"
                          }
                        }
                      },
                      {
                        "dest_site" : {
                          "terms" : {
                            "field" : "dest_site"
                          }
                        }
                      }
                    ]
                  },
                  "aggs": {
                    val_fld: {
                      "avg": {
                        "field": val_fld
                      }
                    }
                  }
                }
              }
            }


#     print(idx, str(query).replace("\'", "\""))
    aggrs = []

    aggdata = hp.es.search(index=idx, body=query)
    for item in aggdata['aggregations']['groupby']['buckets']:
        aggrs.append({'pair': str(item['key']['src']+'-'+item['key']['dest']),
                      'from':dateFrom, 'to':dateTo,
                      'src': item['key']['src'], 'dest': item['key']['dest'],
                      'src_host': item['key']['src_host'], 'dest_host': item['key']['dest_host'],
                      'src_site': item['key']['src_site'], 'dest_site': item['key']['dest_site'],
                      'value': item[val_fld]['value'],
                      'doc_count': item['doc_count']
                     })

    return aggrs


def get_ip_host(idx, dateFrom, dateTo):
    def q_ip_host (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        },
                        {
                          "term" : {
                            "src_production" : True
                          }
                        },
                        {
                          "term" : {
                            "dest_production" : True
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_host") : {
                              "terms" : {
                                "field" : str(fld+"_host"),
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }

    res_ip_host = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_host(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            host = item['key'][str(field+'_host')]
            if ((ip in res_ip_host.keys()) and (host is not None) and (host != ip)) or (ip not in res_ip_host.keys()):
                res_ip_host[ip] = host
    return res_ip_host


def get_ip_site(idx, dateFrom, dateTo):
    def q_ip_site (fld):
        return {
                  "size" : 0,
                  "query" : {  
                    "bool" : {
                      "must" : [
                        {
                          "range" : {
                            "timestamp" : {
                              "from" : dateFrom,
                              "to" : dateTo
                            }
                          }
                        },
                        {
                          "term" : {
                            "src_production" : True
                          }
                        },
                        {
                          "term" : {
                            "dest_production" : True
                          }
                        }
                      ]
                    }
                  },
                  "_source" : False,
                  "stored_fields" : "_none_",
                  "aggregations" : {
                    "groupby" : {
                      "composite" : {
                        "size" : 9999,
                        "sources" : [
                          {
                            fld : {
                              "terms" : {
                                "field" : fld,
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            str(fld+"_site") : {
                              "terms" : {
                                "field" : str(fld+"_site"),
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          },
                          {
                            "ipv6" : {
                              "terms" : {
                                "field" : "ipv6",
                                "missing_bucket" : True,
                                "order" : "asc"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }

    res_ip_site = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_ip_site(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            ip = item['key'][field]
            site = item['key'][str(field+'_site')]
            ipv6 = item['key']['ipv6']
            if ((ip in res_ip_site.keys()) and (site is not None)) or (ip not in res_ip_site.keys()):
                res_ip_site[ip] = [site, ipv6]
    return res_ip_site


def get_host_site(idx, dateFrom, dateTo):
    def q_host_site (fld):
        return {
          "size" : 0,
          "query" : {  
            "bool" : {
              "must" : [
                {
                  "range" : {
                    "timestamp" : {
                      "from" : dateFrom,
                      "to" : dateTo
                    }
                  }
                },
                {
                  "term" : {
                    "src_production" : True
                  }
                },
                {
                  "term" : {
                    "dest_production" : True
                  }
                }
              ]
            }
          },
          "_source" : False,
          "stored_fields" : "_none_",
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    str(fld+"_site") : {
                      "terms" : {
                        "field" : str(fld+"_site"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    str(fld+"_host") : {
                      "terms" : {
                        "field" : str(fld+"_host"),
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }

    res_host_site = {}
    for field in ['src', 'dest']:
        results = hp.es.search(index=idx, body=q_host_site(field))

        for item in results["aggregations"]["groupby"]["buckets"]:
            site = item['key'][str(field+"_site")]
            host = item['key'][str(field+'_host')]
            if ((host in res_host_site.keys()) and (site is not None)) or (host not in res_host_site.keys()):
                res_host_site[host] = site
    return res_host_site


def get_metadata(dateFrom, dateTo):
    def q_metadata():
        return {
          "size" : 0,
          "query" : {
            "range" : {
              "timestamp" : {
                "from" : dateFrom,
                "to" : dateTo
              }
            }
          },
          "_source" : False,
          "aggregations" : {
            "groupby" : {
              "composite" : {
                "size" : 9999,
                "sources" : [
                  {
                    "site" : {
                      "terms" : {
                        "field" : "config.site_name.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_email" : {
                      "terms" : {
                        "field" : "administrator.email",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "admin_name" : {
                      "terms" : {
                        "field" : "administrator.name",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv6" : {
                      "terms" : {
                        "field" : "external_address.ipv6_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "ipv4" : {
                      "terms" : {
                        "field" : "external_address.ipv4_address",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  },
                  {
                    "host" : {
                      "terms" : {
                        "field" : "host.keyword",
                        "missing_bucket" : True,
                        "order" : "asc"
                      }
                    }
                  }
                ]
              }
            }
          }
        }

    results = hp.es.search(index='ps_meta', body=q_metadata())
    res_meta = {}
    for item in results["aggregations"]["groupby"]["buckets"]:
        host = item['key']['host']
        if ((host in res_meta.keys()) and (item['key']['site'] is not None)) or (host not in res_meta.keys()):
            res_meta[host] = {'site': item['key']['site'], 'admin_name': item['key']['admin_name'],
                              'admin_email': item['key']['admin_email'], 'ipv6': item['key']['ipv6'],
                              'ipv4': item['key']['ipv4']}
    return res_meta
