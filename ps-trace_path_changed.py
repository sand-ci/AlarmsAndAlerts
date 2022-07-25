from elasticsearch.helpers import scan
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
import pandas as pd
import json
import requests
import collections

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
                                "lte": dt[1]
                            }
                        }
                    }
                ]
            }
        }
    }
    # print(str(query).replace("\'", "\""))
    try:
        return scan_gen(scan(hp.es, index="ps_trace", query=query, filter_path=['_scroll_id', '_shards', 'hits.hits._source']))
    except Exception as e:
        print(e)


def scan_gen(scan):
    while True:
        try:
            yield next(scan)['_source']
        except:
            break


# gets the data from ES
def ps_trace(dt):
    scan_gen = queryPSTrace(dt)
    items = []

    for meta in scan_gen:
        items.append(meta)

    return items


# create a shared variable to store the data from the parallel execution
manager = Manager()
data = manager.list()


# queries in chunks based on time ranges
def getTraceData(dtRange):
    traceData = ps_trace(dtRange)
    if len(traceData) > 0:
        data.extend(traceData)


# laods the data in parallel
@timer
def runInParallel(dateFrom, dateTo):
    # query the past 12 hours and split the period into 8 time ranges
    # dateFrom, dateTo = hp.defaultTimeRange(12)
    # dateFrom, dateTo = ['2022-05-17 20:15', '2022-05-18 08:15']
    print(f' Run for period: {dateFrom}  -   {dateTo}')
    dtList = hp.GetTimeRanges(dateFrom, dateTo, 12)
    with ProcessPoolExecutor(max_workers=4) as pool:
        result = pool.map(getTraceData, [[dtList[i], dtList[i+1]] for i in range(len(dtList)-1)])


# The traceroute measures provide a list of IP adresses
# as well as a list of correcponding AS numbers
# Below we map IP->ASN and ASN->IP
@timer
def mapHopsAndASNs(df):

    asn2Hop, hop2ASN = {}, {}
    strange = []

    pch, ipl = [], []
    subset = df[['asns', 'hops', 'pair', 'ttls']].values.tolist()
    # max_ttl is needed when we later build a dataframe where each column is a ttl number
    max_ttl = 0

    for asns, hops, pair, ttls in subset:
        if ttls:
            if max(ttls) > max_ttl:
                max_ttl = max(ttls)

        if len(asns) == len(hops):
            for i in range(len(asns)):
                if asns[i] not in asn2Hop.keys():
                    asn2Hop[asns[i]] = [hops[i]]
                else:
                    temp = asn2Hop[asns[i]]
                    if hops[i] not in temp:
                        temp.append(hops[i])
                        asn2Hop[asns[i]] = temp

                if hops[i] not in hop2ASN.keys():
                    hop2ASN[hops[i]] = [asns[i]]
                else:
                    temp = hop2ASN[hops[i]]
                    if asns[i] not in temp:
                        temp.append(asns[i])
                        hop2ASN[hops[i]] = temp
        else:
            print('Size of hops and ASNs differ. This should not happen')
            strange.append([pair, asns, hops])

    return asn2Hop, hop2ASN, max_ttl


# Sometimes the AS number is not available and a 0 is stored instead
# However, we can repair part of the information by looking up the IP address at that position
@timer
def fix0ASNs(df):

    fix_asns, zfix = [], []
    relDf = df[['src', 'dest', 'asns', 'hops', 'pair',
                'destination_reached', 'timestamp', 'ttls']].copy()
    all_reachedDf = relDf.groupby('pair')[['destination_reached']].apply(lambda x: all(
        x.values)).to_frame().rename(columns={0: 'all_dests_reached'}).reset_index()

    relDf = pd.merge(relDf, all_reachedDf, on='pair', how='left')

    relDf['asns_updated'] = None
    relDf['hops_updated'] = None

    try:
        for idx, asns, hops, s, d, destination_reached in relDf[['asns', 'hops', 'src', 'dest', 'all_dests_reached']].itertuples():
            asns_updated = asns.copy()
            hops_updated = hops.copy()
            # print(idx, asns, hops, s, d, destination_reached)

            for pos, n in enumerate(asns):
                # when AS number is 0 (unknown) get the IP at this position and find all ASNs for it, usually it is just 1
                if n == 0:
                    # print(n)
                    ip = hops[pos]
                    asns4IP = hop2ASN[ip]

                    if 0 in asns4IP:
                        asns4IP.remove(0)

                    asns_updated[pos] = n
                    if len(asns4IP) == 1:
                        asns_updated[pos] = asns4IP[0]
                        if idx not in zfix:
                            zfix.append(idx)
                    elif len(asns4IP) > 1:
                        for ip in asns4IP:
                            if len(asns4IP) == 2:
                                alt = [el for el in asns4IP if el != n]
                                # altASNsDict[asn] = alt[0]
                                asns_updated[pos] = alt[0]
                                if idx not in zfix:
                                    zfix.append(idx)
                                # print(n,'.............', alt[0])
                            elif len(hop2ASN[ip]) > 2:
                                print('There are more possibilities ........................',
                                      idx, asns, pos, alt)

            relDf.at[idx, 'asns_updated'] = asns_updated
            # print(set(asns))
            # print(set(asns_updated))
            relDf.at[idx, 'hops_updated'] = hops_updated
            # print()
            # print(set(hops))
            # print(set(hops_updated))

        print(f'{len(zfix)} zeros successfully replaced with AS numbers.')
        return relDf

    except Exception as e:
        print('error', e)
        print(idx, asns, hops, s, d, destination_reached)


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
                if rcsites[rcsite]['netroutes'][netroute]["netsite"] == netsite or rcsites[rcsite]['netroutes'][netroute]["netsite_spare"] == netsite:
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
def getAltASNs(asn2Hop, hop2ASN):
    alt_dict = {}
    for asn, l in asn2Hop.items():
        if asn != 0:
            for ip in l:

                others = hop2ASN[ip]
                if 0 in others:
                    others.remove(0)

                if len(others) == 2:
                    # if 0 not in hop2ASN[ip]:
                    alt = [el for el in hop2ASN[ip] if el != asn]
                    if len(alt) > 1:
                        print(f'There are >1 alternatives to {asn}: {alt}')

                    altASN = [alt[0]]
                    if asn in alt_dict.keys():
                        temp = alt_dict[asn]
                        if alt[0] not in temp:
                            temp.append(alt[0])
                        altASN = temp

                    alt_dict[asn] = altASN
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
        for aslist in alt_dict.values():
            temp = []
            if key in aslist and len(aslist) > 1:
                temp = aslist.copy()
                allVals.extend(list(set(temp)))

        allVals = list(set(allVals))
        if key in allVals:
            allVals.remove(key)
        altsOfAlts[key] = allVals

    return altsOfAlts


# Adds known ASNs manuaaly
def mapASNsManualy(asn1, asn2):
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
                for g in group.asns_updated.values:
                    if g is not None and g == g:
                        asnList = list(set(g.copy()))

                    if 0 in asnList:
                        asnList.remove(0)

                    hashid = hash(frozenset(asnList))

                    if hashid not in hashList:
                        hashList.append(hashid)

                        if len(g) > 0:
                            uniquePathsList.append([group.name[0], group.name[1], asnList, len(
                                asnList), len(group.values), hashid, group.all_dests_reached.values[0]])

                    if len(g) > 0:
                        allPathsList.append([group.name[0], group.name[1],
                                            asnList, len(asnList), len(group.values), hashid])

        except Exception as e:
            print('Issue wtih:', group.name, asnList)
            print(e)

    relDf[['src', 'dest', 'asns_updated', 'hops', 'all_dests_reached']
          ].groupby(['src', 'dest']).apply(lambda x: hashASNs(x))

    cleanPathsDf = pd.DataFrame(uniquePathsList).rename(columns={
        0: 'src', 1: 'dest', 2: 'asns_updated', 3: 'cnt_asn', 4: 'cnt_total_measures', 5: 'hash', 6: 'all_dests_reached'})
    cleanPathsDf['pair'] = cleanPathsDf['src']+'-'+cleanPathsDf['dest']
    cleanPaths = pd.DataFrame(allPathsList).rename(
        columns={0: 'src', 1: 'dest', 2: 'asns_updated', 3: 'cnt_asn', 4: 'cnt_total_measures', 5: 'hash'})

    pathFreq = cleanPaths.groupby(['src', 'dest'])['hash'].apply(
        lambda x: x.value_counts(normalize=True))
    pathFreq = pd.DataFrame(pathFreq).reset_index().rename(
        columns={'hash': 'hash_freq', 'level_2': 'hash'})

    pathDf = pd.merge(cleanPathsDf, pathFreq, how="inner", on=['src', 'dest', 'hash'])
    sub = df[['dest', 'src_site', 'src', 'dest_site',
              'src_host', 'dest_host', 'pair']].drop_duplicates()
    pathDf = pd.merge(pathDf, sub, on=['pair', 'src', 'dest'], how='left')

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

            # if path was used 65% of the time, use it as a baseline
            if freq_max >= 0.65:
                diversity = 0
                position = freq_max_position[0]
                for pos in freq_max_position:
                    if diversity < group.cnt_asn.values[pos]:
                        position = pos
                        diversity = group.cnt_asn.values[pos]
                max_position = [position]
                freq_max = group.hash_freq.values[position]
            # if not, get the bath with the highest number of unique ASNs
            else:
                if len(cnt_max_position) > 1:
                    path_freq = 0
                    for pos in cnt_max_position:
                        if path_freq < group.hash_freq.values[pos]:
                            position = pos
                            path_freq = group.hash_freq.values[pos]
                    max_position = [position]
                else:
                    max_position = cnt_max_position

            baselineList.append(group.index.values[max_position[0]])

        except Exception as e:
            print('Issue wtih:', group, e)

    # the dataframe conatingn one path as a baseline per pair
    baseLine = dd[dd.index.isin(baselineList)].copy()
    # the dataframe containing all the remaining paths
    compare2 = dd[~dd.index.isin(baseLine.index)].copy()
    print(f' Baseline: {len(baseLine)} \n left to compare to: {len(compare2)}')

    return [baseLine, compare2]


# Compares each path to the baseline. Does that for each pair and flags the ASN that are not on the baseline path
# Returns a dictionary of pairs and a list of flagged AS numbers
@timer
def getChanged(baseDf, compare2, updatedbaseLine, altsOfAlts, cricDict):

    diffs = {}

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
                freq = group['hash_freq'].values[i]
                diff = list(set(asns)-set(base))

                if len(diff) > 0:

                    for d in diff:

                        if not d in upbase:
                            if d in altsOfAlts.keys():
                                # if none of the alternative ASNs is in the baseline path, then flag it to True (meaning raise an alarm)
                                flag = not any(
                                    False if alt not in base or alt in upbase else True for alt in altsOfAlts[d])
                                # print(flag)

                            elif d in cricDict.keys():
                                flag = not any(
                                    False if alt not in base or alt in upbase else True for alt in cricDict[d])

                            else:
                                flag = True

                        alarms.append(flag)

                    diff_temp.extend(diff)

            if any(alarms) > 0 and len(base) >= 2:
                diffs[name] = list(set(diff_temp))
        except Exception as e:
            print('Issue wtih:', name, e)

    print(f'Number of pairs flagged {len(diffs)}')

    return diffs


# Builds a dataframe that takes into account the missing TTLs
@timer
def positionASNsUsingTTLs(pairs, relDf, max_ttl):
    subset = relDf[relDf['pair'].isin(
        pairs)][['asns_updated', 'hops', 'pair', 'ttls']].values.tolist()

    columns = ['pair', 'max_ttl']
    columns.extend(range(1, max_ttl+1))
    posDf = pd.DataFrame(columns=columns)

    for asns, hops, pair, ttls in subset:
        if len(asns) == len(hops) and ttls:
            idx = len(posDf)
            posDf.loc[idx, ttls] = asns
            missing = {x: -1 for x in range(ttls[0], ttls[-1]+1) if x not in ttls}
            posDf.loc[idx, list(missing.keys())] = list(missing.values())
            posDf.loc[idx, 'pair'] = pair

    return posDf


# Gets the probability of an ASN to apear at each position on the path
@timer
def getProbabilities(posDf, max_ttl):
    plist = []

    def calcP(g):
        pair = g.pair.values[0]
        for col in range(1, max_ttl+1):
            asns = g[col].value_counts('probability').index.values
            p = g[col].value_counts('probability').values

            for i, asn in enumerate(asns):
                plist.append([pair, asn, col, p[i]])

    posDf.groupby('pair').apply(lambda g: calcP(g))

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

    for asn, g in diffDf[diffDf['diff'].isin(top.index)][['diff', 'src_site', 'dest_site']].drop_duplicates().groupby('diff'):
        affectedSites = list(set(g['src_site'].values.tolist() + g['dest_site'].values.tolist()))

        owner = asnInfo[str(asn)] if str(asn) in asnInfo.keys() else ''
        alarmsList.append({'asn': asn,
                           'owner': asnInfo[str(asn)],
                           'num_pairs': str(top[top.index == asn]['pair'].values[0]),
                           'sites': affectedSites,
                           'from': dateFrom,
                           'to': dateTo})

    return alarmsList


# Fill in hosts and site names where missing by quering the ps_alarms_meta index
@timer
def fixMissingMetadata(rawDf):
    metaDf = qrs.getMetaData()
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='src', how='right').rename(
                columns={'host':'host_src','site':'site_src'}).drop(columns=['ip'])
    rawDf = pd.merge(metaDf[['host', 'ip', 'site']], rawDf, left_on='ip', right_on='dest', how='right').rename(
                columns={'host':'host_dest','site':'site_dest'}).drop(columns=['ip'])

    rawDf['src_site'] = rawDf['src_site'].fillna(rawDf.pop('site_src'))
    rawDf['dest_site'] = rawDf['dest_site'].fillna(rawDf.pop('site_dest'))
    rawDf['src_host'] = rawDf['src_host'].fillna(rawDf.pop('host_src'))
    rawDf['dest_host'] = rawDf['dest_host'].fillna(rawDf.pop('host_dest'))

    return rawDf


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


# Sends the alarms
@timer
def sendAlarms(data):
    ALARM = alarms('Networking', 'RENs', 'path changed')
    for issue in data:
        # print(issue)
        ALARM.addAlarm(
            body="Path changed",
            tags=issue['sites'],
            source=issue
        )


# query the past 12 hours and split the period into 8 time ranges
dateFrom, dateTo = hp.defaultTimeRange(12)
# dateFrom, dateTo = ['2022-05-25 09:40', '2022-05-25 21:40']
runInParallel(dateFrom, dateTo)
df = pd.DataFrame(list(data))
df['pair'] = df['src']+'-'+df['dest']

df = fixMissingMetadata(df)
asn2Hop, hop2ASN, max_ttl = mapHopsAndASNs(df)

cricDict = getCricASNInfo()
altASNsDict = getAltASNs(asn2Hop, hop2ASN)
altsOfAlts = getAltsOfAlts(altASNsDict)

mapASNsManualy(513, 20969)
mapASNsManualy(20969, 513)
mapASNsManualy(291, 293)
mapASNsManualy(293, 291)

relDf = fix0ASNs(df)
pathDf = getStats4Paths(relDf, df)

# remove rows where site is None and ignore those with 100% stable paths
valid = pathDf[~(pathDf['src_site'].isnull()) & ~(
    pathDf['dest_site'].isnull()) & (pathDf['hash_freq'] < 1)].copy()
if len(valid) == 0:
    raise NameError('No valid paths. Check pathDf.')
baseLine, compare2 = getBaseline(valid)

# get a second stable path (baseline) for the T1 sites
T1 = ['BNL-ATLAS', 'FZK-LCG2', 'IN2P3-CC', 'INFN-T1', 'JINR-T1', 'KR-KISTI-GSDC-01', 'NDGF-T1', 'NIKHEF-ELPROD',
      'pic', 'RAL-LCG2', 'RRC-KI-T1', 'SARA-MATRIX', 'Taiwan-LCG2', 'TRIUMF-LCG2', 'USCMS-FNAL-WC1']
t1s = compare2[(compare2['src_site'].isin(T1)) & (compare2['dest_site'].isin(T1))]
updatedbaseLine, updatedcompare2 = getBaseline(t1s)


# Ignore sites for which we know there's an issue
ignore_list = ['ATLAS-CBPF', 'NCP-LCG2', 'UTA_SWT2', 'RRC_KI', 'CBPF', 'IN2P3-CC', 'JINR-LCG2',
               'JINR-T1', 'RRC-KI-T1', 'RRC-KI', 'ITEP', 'RU-Protvino-IHEP', 'BEIJING-LCG2']
cut = compare2[(~compare2['src_site'].isin(ignore_list)) &
               (~compare2['dest_site'].isin(ignore_list))]

# Get the pairs which took different form the usual paths
diffs = getChanged(baseLine, compare2, updatedbaseLine, altsOfAlts, cricDict)

# Build a position matrix, where each TTL helps put ASNs at their places
posDf = positionASNsUsingTTLs(diffs.keys(), relDf, max_ttl)
# Get the probability for each position, based on src-dest pair
probDf = getProbabilities(posDf, max_ttl)


# Find the nodes that work sometimes and add those the the baseline list
baseLine = addOnAndOffNodes(diffs, probDf, baseLine)
# Again get the pairs which took different form the usual paths
diffs = getChanged(baseLine, compare2, updatedbaseLine, altsOfAlts, cricDict)

# Extract all seen ASNs
asns = list(set([str(item) for l in diffs.values() for item in l]))
# Get the oweners of the ASNs
asnInfo = getASNInfo(asns)
# Build the dictinary of alarms where for each ASN, there is an owner, number of pairs and a list of affected sites
alarmsList = aggResultsBasedOnSites(diffs, asnInfo, dateFrom, dateTo)

sendAlarms(alarmsList)
