from elasticsearch.helpers import scan, parallel_bulk
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import json
import requests
import collections
import hashlib
import traceback

import utils.helpers as hp
import utils.queries as qrs
from utils.helpers import timer
from alarms import alarms


# Builds the trceroute query
def queryPSTrace(dt):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gt": dt[0],
                                "lte": dt[1],
                                "format": "epoch_millis"
                            }
                        }
                    }
                ]
            }
        }
    }
    # print(str(query).replace("\'", "\""))
    try:
        return scan_gen(scan(hp.es, index="ps_trace", query=query,
                             filter_path=['_scroll_id', '_shards', 'hits.hits._source'],
                             _source=['timestamp', 'src_netsite', 'dest_netsite', 'src', 'dest',  'src_host', 'dest_host', \
                                      'destination_reached', 'asns', 'hops', 'pair', 'ttls']))
    except Exception as e:
        print(e)


def scan_gen(scan):
    while True:
        try:
            yield next(scan)['_source']
        except Exception:
            break


# gets the data from ES
def ps_trace(dt):
    scan_gen = queryPSTrace(dt)
    items = []

    for meta in scan_gen:
        items.append(meta)

    return items


# queries in chunks based on time ranges
def getTraceData(dtRange):
    traceData = ps_trace(dtRange)
    if len(traceData) > 0:
        print(f'For {dtRange} fetched: {len(traceData)}')
    return traceData


# laods the data in parallel
@timer
def runInParallel(dateFrom, dateTo):
    # query the past 12 hours and split the period into 8 time ranges
    # dateFrom, dateTo = hp.defaultTimeRange(12)
    # dateFrom, dateTo = ['2022-05-17 20:15', '2022-05-18 08:15']
    print(f' Run for period: {dateFrom}  -   {dateTo}')
    dtList = hp.GetTimeRanges(dateFrom, dateTo, 12)
    result = []
    with ProcessPoolExecutor(max_workers=4) as pool:
        result.extend(pool.map(getTraceData, [[dtList[i], dtList[i+1]] for i in range(len(dtList)-1)]))

    data = []
    for d in result:
        data.extend(d)
    return data

# The traceroute measures provide a list of IP adresses
# as well as a list of correcponding AS numbers
# Below we map IP->ASN and ASN->IP
@timer
def mapHopsAndASNs(df):

    asn2ip, ip2asn = {}, {}
    strange = []

    subset = df[['asns', 'hops', 'pair', 'ttls']].values.tolist()
    # max_ttl is needed when we later build a dataframe where each column is a ttl number
    max_ttl = 0
    try:
        for asns, hops, pair, ttls in subset:
            if ttls:
                if max(ttls) > max_ttl:
                    max_ttl = max(ttls)

            if len(asns) == len(hops):
                for i in range(len(asns)):
                    if asns[i] not in asn2ip.keys():
                        asn2ip[asns[i]] = [hops[i]]
                    else:
                        temp = asn2ip[asns[i]]
                        if hops[i] not in temp:
                            temp.append(hops[i])
                            asn2ip[asns[i]] = temp

                    if hops[i] not in ip2asn.keys():
                        ip2asn[hops[i]] = []

                    if asns[i] not in ip2asn[hops[i]]:
                        ip2asn[hops[i]].append(asns[i])

            else:
                print('Size of hops and ASNs differ. This should not happen')
                strange.append([pair, asns, hops])

    except Exception as e:
        print(e)
        print(asns, hops, pair, ttls)

    return asn2ip, ip2asn, max_ttl


# Sometimes the AS number is not available and a 0 is stored instead
# However, we can repair part of the information by looking up the IP address at that position

@timer
def fix0ASNs(df):

    zfix = []
    relDf = df[['src', 'dest', 'asns', 'hops', 'pair',
                'destination_reached', 'timestamp', 'ttls']].copy()

    relDf['asns_updated'] = relDf['asns']

    # print('Attempt to fix unknown ASNs based on mapped IP addresses...')
    c = 0

    try:
        for idx, asns, hops, s, d in relDf[['asns', 'hops', 'src', 'dest']].itertuples():

            if len(asns)>0 and 0 in asns:
                asns_updated = asns.copy()

                positions = [pos for pos, n in enumerate(asns) if n==0]

                for pos in positions:
                    # when AS number is 0 (unknown) get the IP at this position and
                    # find all ASNs for it, usually it is just 1
                    ip = hops[pos]
                    asns4IP = ip2asn[ip]

                    if 0 in asns4IP:
                        asns4IP.remove(0)

                    if asns4IP:
                        if len(asns4IP) < 3:
                            # replace 0 with the known ASN for that IP
                            asns_updated[pos] = asns4IP[0]
                            # if len(asns4IP) > 1:
                            #     # when there are 2 we add both
                            #     asns_updated.append(asns4IP[1])
                            if idx not in zfix:
                                zfix.append(idx)

                        else:
                            print('Too many possibilities ...', idx, asns, pos)

                relDf.at[idx, 'asns_updated'] = asns_updated

                if c>0 and c%50000 == 0:
                    print(f'Processed {c}', flush=True)
                c+=1

        print(f'{len(zfix)} zeros successfully replaced with AS numbers.', flush=True)
        return relDf
    except Exception as e:
        print(idx, asns, hops, s, d)
        print(e, traceback.format_exc())


# Gets all ASNs for each site name from CRIC
# They will later be used as alternatives to each other
@timer
def getCricASNInfo():

    cricDict = {}

    response = requests.get(
        "https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json", verify=False)
    rcsites = json.loads(response.text)

    cricsites = {}

    for rcsite in rcsites:
        temp = []
        for netsite in rcsites[rcsite]['netsites']:
            for netroute in rcsites[rcsite]['netroutes']:
                if rcsites[rcsite]['netroutes'][netroute]["netsite"] == netsite or \
                   rcsites[rcsite]['netroutes'][netroute]["netsite_spare"] == netsite:

                    for iptype in rcsites[rcsite]['netroutes'][netroute]["networks"]:
                        for subnet in rcsites[rcsite]['netroutes'][netroute]["networks"][iptype]:
                            asn = rcsites[rcsite]['netroutes'][netroute]["asn"]
                            if asn not in temp:
                                temp.append(asn)
            if temp:
                cricsites[rcsite] = temp

    cricDict = {}
    for key, vals in cricsites.items():
        # print(vals)
        if len(vals) > 1:
            for v in vals:
                if v not in cricDict.keys():
                    cricDict[v] = vals.copy()
                    cricDict[v].remove(v)
                    # print(cricDict)
                else:
                    print('                currvals', cricDict[v])

    return cricDict


# Builds a dictionalry of ASNs, where the key is a single AS number and
# the values are the alaternative ASNs dicovered through the hops mapping
@timer
def getAltASNs(asn2ip, ip2asn):
    alt_dict = {}
    for asn, ip_list in asn2ip.items():

        if asn != 0:
            for ip in ip_list:

                others = ip2asn[ip]
                if 0 in others:
                    others.remove(0)

                if len(others) == 2:
                    alt = [el for el in ip2asn[ip] if el != asn]
                    if len(alt) > 1:
                        print(f'There are >1 alternatives to {asn}: {alt}')

                    if asn not in alt_dict.keys():
                        alt_dict[asn] = [alt[0]]
                    else:
                        if alt[0] not in alt_dict[asn]:
                            alt_dict[asn].append(alt[0])

                elif len(others) > 2:
                    print(asn, others)
                    print('There are more possibilities ........................')
    return alt_dict


# Grabs also the alternative ASNs of the altenrnatives
@timer
def getAltsOfAlts(altASNsDict):
    alt_dict = altASNsDict.copy()
    altsOfAlts = {}
    allVals = []
    for key, vals in alt_dict.items():
        allVals = vals
        # print(key, vals)
        for asn_list in alt_dict.values():
            temp = []
            if key in asn_list and len(asn_list) > 1:
                temp = asn_list.copy()
                allVals.extend(list(set(temp)))

        allVals = list(set(allVals))
        if key in allVals:
            allVals.remove(key)
        altsOfAlts[key] = allVals

    return altsOfAlts


# Adds known ASNs manuaaly
def mapASNsManualy(asn1, asn2, altsOfAlts):
    if asn1 not in altsOfAlts.keys():
        altsOfAlts[asn1] = [asn2]
    else:
        temp = altsOfAlts[asn1]
        temp.extend(asn2)
        altsOfAlts[asn1]


# Builds a dataframe that strips all repeated values and
# hashes each clean path
# Also calculates what % a path gets used for the given period
@timer
def getStats4Paths(relDf, df):
    allPathsList, uniquePathsList = [], []

    def hashASNs(group):
        try:
            hashList = []
            if len(group.asns_updated.values) > 1:
                # print(group.asns_updated.values)
                for i, g in enumerate(group.asns_updated.values):
                    if g is not None and g == g:
                        # take only the unique AS numbers on the list
                        asnList = list(dict.fromkeys(g.copy()))

                        # remove remaining zeros (unknowns)
                        if 0 in asnList:
                            asnList.remove(0)

                        # hash the path
                        hashid = hash(frozenset(asnList))

                        if hashid not in hashList:
                            hashList.append(hashid)

                            if len(g) > 0:
                                # store just the unique sequences since
                                # Pandas has limitted functions on dataframes with lists
                                uniquePathsList.append([group.name[0], group.name[1], asnList,
                                                        len(asnList), len(group.values), hashid])
                        if len(g) > 0:
                            # store all values + the cleaned paths and hashes,
                            # so that we can get the probabilities later
                            allPathsList.append([group.name[0], group.name[1], asnList, len(asnList),
                                                len(group.values), hashid, group.destination_reached.values[i]])
        except Exception as e:
            print('Issue wtih:', group.name, asnList)
            print(e)

    relDf[['src', 'dest', 'asns_updated', 'hops', 'destination_reached']].\
        groupby(['src', 'dest'], group_keys=True).apply(lambda x: hashASNs(x))

    uniquePaths = pd.DataFrame(uniquePathsList).rename(columns={
        0: 'src', 1: 'dest', 2: 'asns_updated',
        3: 'cnt_asn', 4: 'cnt_total_measures', 5: 'hash'
    })
    uniquePaths['pair'] = uniquePaths['src']+'-'+uniquePaths['dest']

    cleanPathsAllTests = pd.DataFrame(allPathsList).rename(columns={
        0: 'src', 1: 'dest', 2: 'asns_updated', 3: 'cnt_asn',
        4: 'cnt_total_measures', 5: 'hash', 6: 'dest_reached'
    })

    # for each hashed path check if all tests reported destination_reached=True
    pathReachedDestDf = cleanPathsAllTests.groupby('hash', group_keys=True).\
        apply(lambda x: True if all(x.dest_reached) else False).\
        to_frame().rename(columns={0: 'path_always_reaches_dest'})

    # get the probability for each path in a column (hash_freq)
    pathFreq = cleanPathsAllTests.groupby(['src', 'dest'], group_keys=True)['hash'].\
        apply(lambda x: x.value_counts(normalize=True)).to_frame()
    pathFreq = pathFreq.reset_index().rename(columns={'hash': 'hash_freq', 'level_2': 'hash'})

    # finally merge with the rest of the dataframes in order to add all available fields
    pathDf = pd.merge(uniquePaths, pathFreq, how="inner", on=['src', 'dest', 'hash'])
    sub = df[['dest', 'src_site', 'src', 'dest_site', 'src_host', 'dest_host', 'pair']].drop_duplicates()
    pathDf = pd.merge(pathDf, sub, on=['pair', 'src', 'dest'], how='inner').drop_duplicates(subset=['hash', 'pair'])
    pathDf = pd.merge(pathDf, pathReachedDestDf, how="left", on=['hash'])

    return pathDf


# Takes one path as a baseline depending on the % usage and the number of unique ASNs
# Separates those from the rest of the paths.
# Returns the 2 dataframes
@timer
def getBaseline(dd):
    baselineList = []

    for name, group in dd.groupby('pair'):
        try:
            cnt_max = max(group.cnt_asn.values)
            freq_max = max(group.hash_freq.values)

            cnt_max_position = [pos for pos, i in enumerate(group.cnt_asn) if i == cnt_max]
            freq_max_position = [pos for pos, i in enumerate(group.hash_freq) if i == freq_max]

            max_position = -9999

            # if path was used 65% of the time, take it for a baseline
            if freq_max >= 0.65:
                position = freq_max_position[0]
                max_position = position
                freq_max = group.hash_freq.values[position]
            # if not, get the path with the highest count of unique ASNs
            else:
                # in case there are >1 paths with the same # of ASNs, take the one most frequently taken
                if len(cnt_max_position) > 1:
                    path_freq = 0
                    for pos in cnt_max_position:
                        if path_freq < group.hash_freq.values[pos]:
                            position = pos
                            path_freq = group.hash_freq.values[pos]
                    max_position = position
                else:
                    max_position = cnt_max_position[0]

            baselineList.append(group.index.values[max_position])

        except Exception as e:
            print('EXCEPTION:', e, name)
            print()
            print(group.cnt_asn.values)
            print(group.hash_freq.values)
            print(group.index.values[max_position])
            print(max_position, group.index.values[max_position], group.index.values[max_position])

    # the dataframe conatingn one path as a baseline per pair
    baseLine = dd[dd.index.isin(baselineList)].copy()
    # the dataframe containing all the remaining paths
    compare2 = dd[~dd.index.isin(baseLine.index)].copy()
    print(f' Baseline: {len(baseLine)} \n left to compare to: {len(compare2)}')

    return [baseLine, compare2]


# Compares each path to the baseline. Does that for each pair and flags the ASN that are not on the baseline path
# Returns a dictionary of pairs and a list of flagged AS numbers
@timer
def getChanged(baseDf, compare2, updatedbaseLine, altsOfAlts, cricDict, cut):

    diffs = {}

    # look at all traceroute tests for each pair
    for name, group in cut.groupby('pair'):
        try:
            base = baseDf[baseDf['pair'] == name]['asns_updated'].values.tolist()[0]

            upbase = []
            if name in updatedbaseLine['pair'].values:
                upbase = updatedbaseLine[updatedbaseLine['pair'] == name]['asns_updated'].values.tolist()[
                    0]

            casns = compare2[compare2['pair'] == name]['asns_updated'].tolist()
            asns_expanded = list([j for i in casns for j in i])
            counter = collections.Counter(asns_expanded)

            flag = False
            diff_temp = []

            alarms = []
            for i, asns in enumerate(group.asns_updated):
                diff = list(set(asns)-set(base))

                if len(diff) > 0:

                    for d in diff:

                        if d not in upbase:
                            if d in altsOfAlts.keys():
                                # if none of the alternative ASNs is in the baseline path or the updated baseline list,
                                # then flag it to True (meaning raise an alarm)
                                flag = not any(False if alt not in base or alt in upbase else True
                                               for alt in altsOfAlts[d])
                                # print(flag)

                            elif d in cricDict.keys():
                                # some ASN alternatives are found in CRIC, if that's the case,
                                # there is no need for an alarm
                                flag = not any(False if alt not in base or alt in upbase else True
                                               for alt in cricDict[d])

                            else:
                                flag = True

                        # store the flags
                        alarms.append(flag)

                    # store the ASNs not on the baseline list or on the alternative ASN lists
                    diff_temp.extend(diff)

            # exclude paths having <3 hops, and check if any flags were raised
            if any(alarms) > 0 and len(base) >= 2:
                # store the pair and the list of diffs
                diffs[name] = list(set(diff_temp))
        except Exception as e:
            print('Issue wtih:', name, e)

    print(f'Number of pairs flagged {len(diffs)}')

    return diffs


# Builds a dataframe that takes into account the missing TTLs
@timer
def positionASNsUsingTTLs(subset):
    pdf = pd.DataFrame()
    for idx, asns, hops, pair, ttls in subset.itertuples():
        if len(asns) == len(hops) and ttls:
            pdf.loc[idx, ttls] = asns
            missing = {x: -1 for x in range(ttls[0], ttls[-1]+1) if x not in ttls}
            pdf.loc[idx, list(missing.keys())] = list(missing.values())
            pdf.loc[idx, 'pair'] = pair
    return pdf

# Gets the probability of an ASN to apear at each position on the path
@timer
def getProbabilities(posDf, max_ttl):
    columns = ['pair']
    columns.extend(sorted([c for c in posDf.columns if c != 'pair']))
    posDf = posDf[columns].sort_values('pair')

    plist = []

    def calcP(g):
        try:
            pair = g.pair.values[0]
            for col in range(1, len(g.columns)):

                asns = g[col].value_counts('probability').index.values
                p = g[col].value_counts('probability').values

                for i, asn in enumerate(asns):
                    plist.append([pair, asn, col, p[i]])
        except Exception as e:
            print(pair)
            print(e)

    posDf.groupby('pair', group_keys=True).apply(lambda g: calcP(g))


    return pd.DataFrame(plist, columns=['pair', 'asn', 'pos', 'P'])


# In some cases a devixe on the path could be mostly down and only sometimes reponds.
# The code bellow finds those and adds the "blinking" ASN to the baseline
@timer
def addOnAndOffNodes(diffs, probDf, baseLine):

    for p in diffs.keys():
        sub = probDf[(probDf['pair'] == p)]
        onoff, currASNs = [], []

        for d in diffs[p]:
            pos = sub[(sub['asn'] == d)]['pos'].values[0]
            asns = sub[sub['pos'] == pos]['asn'].values

            # if there are only 2 values at that position
            # one is the flagged ASN and the other one is -1,
            # meaning the ttl was missing,
            # then the node was On and Off and should not be considered as path change
            if len(asns) == 2 and -1 in asns:
                onoff.append(sorted(asns)[1])

        onoff = list(set(onoff))
        if onoff:
            currASNs = baseLine[baseLine['pair'] == p]['asns_updated'].values.tolist()[0].copy()
            currASNs.extend(onoff)
            baseLine.at[baseLine[baseLine['pair'] == p].index.values[0], 'asns_updated'] = currASNs

    return baseLine


# Builds a dataframe based on the flagged ASNs when tests take another route (change path),
# the number of pairs between which the ASN appears, affected sites, AS owner and the queried period
@timer
def aggResultsBasedOnSites(diffs, asnInfo, dateFrom, dateTo):

    diffData = []
    for pair, diff in diffs.items():
        for d in diff:
            diffData.append([pair, d])

    diffDf = pd.DataFrame(diffData, columns=['pair', 'diff'])

    sub = df[['dest', 'src_site', 'src', 'dest_site',
              'src_host', 'dest_host', 'pair']].drop_duplicates()
    diffDf = pd.merge(diffDf, sub, on='pair', how='left')
    cntPairs = diffDf.groupby(['diff'])[['pair']].count().sort_values('pair', ascending=False)

    top = cntPairs[cntPairs['pair'] >= 10]

    # n = 80 if len(allPairs)>80 else len(allPairs)
    # pairs2Plot = allPairs.sample(n=n, random_state=1).values

    alarmsList = []

    for asn, g in diffDf[diffDf['diff'].isin(top.index)][['diff', 'src_site', 'dest_site']].\
            drop_duplicates().groupby('diff'):

        affectedSites = list(set(g['src_site'].values.tolist() + g['dest_site'].values.tolist()))
        toHash = ','.join([str(asn), dateFrom, dateTo])
        alarm_id = hashlib.sha224(toHash.encode('utf-8')).hexdigest()

        alarmsList.append({
            'asn': asn,
            'owner': asnInfo[str(asn)],
            'num_pairs': str(top[top.index == asn]['pair'].values[0]),
            'sites': affectedSites,
            'from': dateFrom,
            'to': dateTo,
            'alarm_id': alarm_id
        })

    return alarmsList

# Grabs the R&E ASNs and their owners from ES
@timer
def getASNInfo(ids):

    query = {
        "query": {
            "terms": {
                "_id": ids
            }
        }
    }

    # print(str(query).replace("\'", "\""))
    asnDict = {}
    data = scan(hp.es, index='ps_asns', query=query)
    for item in data:
        asnDict[str(item['_id'])] = item['_source']['owner']

    return asnDict

@timer
def saveStats(diffs, ddf, probDf, baseLine, updatedbaseLine, compare2):
    def getPaths(fld, ddf):
        temp = {}
        ddf.loc[:, 'hash_freq'] = ddf['hash_freq'].round(2)
        if len(ddf)>0:
            temp[fld] = ddf[['asns_updated', 'cnt_total_measures', 'path_always_reaches_dest', 'hash_freq']].\
                to_dict('records')
        return temp

    probDf['P'] = probDf['P'].round(2)
    # Replace invalid values with NaN, then convert to integers
    probDf['asn'] = pd.to_numeric(probDf['asn'], errors='coerce')
    probDf['asn'] = probDf['asn'].astype('int')

    alarmsData = []
    for pair, diff in diffs.items():
        temp = {}
        # prepare the data for ES - adding _id and _index to send in bulk
        temp['from_date'] = dateFrom
        temp['to_date'] = dateTo
        temp['_index'] = 'ps_traces_changes'
        temp['diff'] = diff
        temp.update(baseLine[baseLine['pair']==pair]
                            [['src', 'dest', 'src_host', 'dest_host', 'src_site', 'dest_site']].to_dict('records')[0])

        temp.update(getPaths('baseline', baseLine[baseLine['pair']==pair]))
        temp.update(getPaths('second_baseline', updatedbaseLine[updatedbaseLine['pair']==pair]))
        temp.update(getPaths('alt_paths', compare2[compare2['pair']==pair]))
        temp['positions'] = probDf[probDf['pair']==pair][['asn', 'pos', 'P']].to_dict('records')

        alarmsData.append(temp)


    print(f'Number of docs: {len(alarmsData)}')

    def genData(data):
        for d in data:
            yield d

    for success, info in parallel_bulk(hp.es, genData(alarmsData)):
        if not success:
            print('A document failed:', info)

# Sends the alarms
@timer
def sendAlarms(data):
    ALARM = alarms('Networking', 'RENs', 'path changed')

    for issue in data:
        ALARM.addAlarm(
            body="Path changed",
            tags=issue['sites'],
            source=issue
        )


# query the past 72 hours and split the period into 8 time ranges
dateFrom, dateTo = hp.defaultTimeRange(72)
dateFrom, dateTo ='2024-10-09T09:56:18.000Z','2024-10-12T09:56:18.000Z'
data = runInParallel(dateFrom, dateTo)
df = pd.DataFrame(data)
df = df[~(df['src']=='') & ~(df['dest']=='')]

print('Total number of documnets:', len(df))
df.loc[:, 'src_site'] = df['src_netsite'].str.upper()
df.loc[:, 'dest_site'] = df['dest_netsite'].str.upper()
df.loc[:, 'pair'] = df['src']+'-'+df['dest']
df = df[~(df['src_site'].isnull()) & ~(df['dest_site'].isnull()) & ~(df['asns'].isnull())]
print(f'number of documents: {len(df)}')

asn2ip, ip2asn, max_ttl = mapHopsAndASNs(df)

cricDict = getCricASNInfo()
altASNsDict = getAltASNs(asn2ip, ip2asn)
altsOfAlts = getAltsOfAlts(altASNsDict)

mapASNsManualy(291, 293, altsOfAlts)
mapASNsManualy(293, 291, altsOfAlts)

relDf = hp.parallelPandas(fix0ASNs)(df)
pathDf = getStats4Paths(relDf, df)

# remove rows where site is None and ignore those with 100% stable paths
valid = pathDf[~(pathDf['src_site'].isnull()) & ~(pathDf['dest_site'].isnull()) & (pathDf['hash_freq'] < 1)].copy()
if len(valid) == 0:
    raise NameError('No valid paths. Check pathDf.')
baseLine, compare2 = getBaseline(valid)

# get a second stable path (baseline) for the T1 sites
# T1 = ['BNL-ATLAS', 'FZK-LCG2', 'IN2P3-CC', 'INFN-T1', 'JINR-T1', 'KR-KISTI-GSDC-01', 'NDGF-T1', 'NIKHEF-ELPROD',
#       'pic', 'RAL-LCG2', 'RRC-KI-T1', 'SARA-MATRIX', 'Taiwan-LCG2', 'TRIUMF-LCG2', 'USCMS-FNAL-WC1']
# limit t emporarily to the known site having load balancing
T1 = ['PIC', 'TAIWAN-LCG2']
t1s = compare2[(compare2['src_site'].isin(T1)) & (compare2['dest_site'].isin(T1))]
updatedbaseLine, updatedcompare2 = getBaseline(t1s)


# Ignore sites for which we know there's an issue
ignore_list = [  'ATLAS-CBPF',
                 'BEIJING-LCG2',
                 'CBPF',
                 'EELA-UTFSM',
                 'IHEP',
                 'IN2P3-CC',
                 'ITEP',
                 'ITEP-LHCONE',
                 'JINR-LCG2',
                 'JINR-LCG2-LHCONE',
                 'JINR-T1',
                 'JINR-T1-LHCOPNE',
                 'KHARKOV-KIPT-LCG2-LHCONE',
                 'NCP-LCG2',
                 'RRC-KI',
                 'RRC-KI-T1',
                 'RRC_KI',
                 'RU-PROTVINO-IHEP-LHCONE',
                 'RU-Protvino-IHEP',
                 'UAM-LCG2-LHCONE',
                 'UTA_SWT2']
cut = compare2[(~compare2['src_site'].isin(ignore_list)) & (~compare2['dest_site'].isin(ignore_list))]

# Get the pairs which took different form the usual paths
diffs = getChanged(baseLine, compare2, updatedbaseLine, altsOfAlts, cricDict, cut)

# Build a position matrix, where each TTL helps put ASNs at their places
subset = relDf[relDf['pair'].isin(diffs.keys())][['asns_updated', 'hops', 'pair', 'ttls']]
posDf = hp.parallelPandas(positionASNsUsingTTLs)(subset)

# Get the probability for each position, based on src-dest pair
probDf = getProbabilities(posDf, max_ttl)

# Replace empty strings with NaN first
probDf['asn'].replace('', np.nan, inplace=True)
# Remove rows where 'asn' is NaN (including inf if needed)
probDf = probDf[~probDf['asn'].isna()]

print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")


# Find the nodes that work sporadically and add those the the baseline list
baseLine = addOnAndOffNodes(diffs, probDf, baseLine)
# Again get the pairs which took different form the usual paths
diffs = getChanged(baseLine, compare2, updatedbaseLine, altsOfAlts, cricDict, cut)

saveStats(diffs, df, probDf, baseLine, updatedbaseLine, compare2)

# Extract all seen ASNs
asns = list(set([str(item) for diffList in diffs.values() for item in diffList]))
# Get the oweners of the ASNs
asnInfo = getASNInfo(asns)
# Build the dictinary of alarms where for each ASN, there is an owner, number of pairs and a list of affected sites
alarmsDict = aggResultsBasedOnSites(diffs, asnInfo, dateFrom, dateTo)

sendAlarms(alarmsDict)
