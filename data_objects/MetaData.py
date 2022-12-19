from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
import traceback
import re

import utils.queries as qrs
import utils.helpers as hp


class MetaData(object):

    def __init__(self, dateFrom=None,  dateTo=None):

        metaDf = self.getMetafromES()
        fmt = '%Y-%m-%d %H:%M'
        now = hp.roundTime(datetime.utcnow())
        dateTo = datetime.strftime(now, fmt)

        if len(metaDf) > 1:
            print('Update meta data')
            dateFrom = datetime.strftime(now - timedelta(days=1), fmt)
            timeRange = hp.GetTimeRanges(dateFrom, dateTo)
            latest = self.__getLatestData(timeRange[0], timeRange[1])
            metaDf = self.__updateDataset(metaDf, latest)
        else:
            print('No data found. Query a year back.')
            dateFrom = datetime.strftime(now - timedelta(days=365), fmt)
            timeRange = hp.GetTimeRanges(dateFrom, dateTo, 10)

            metaDf = pd.DataFrame()

            for i in range(len(timeRange)-1):
                print(f'Period: {timeRange[i]}, {timeRange[i+1]}')

                latest = self.__getLatestData(timeRange[i], timeRange[i+1])
                if len(metaDf) > 0:
                    print(f'Size latest data: {len(latest)}')
                    metaDf = self.__updateDataset(metaDf, latest)
                else:
                    metaDf = latest

                print(f'{i}   Size after update: {len(metaDf)}')
                print()

        self.metaDf = metaDf


    @staticmethod
    def getMetafromES():
        meta = []
        data = scan(hp.es, index='ps_alarms_meta')
        for item in data:
            meta.append(item['_source'])

        if meta:
            return pd.DataFrame(meta)


    @staticmethod
    def getNodes(dt):
        allNodesDf = qrs.allTestedNodes(dt)
        allNodesDf = allNodesDf[(allNodesDf['ip'] != '')
                                & ~(allNodesDf['ip'].isnull())]

        # remove the duplicates
        allNodesDf = allNodesDf.sort_values(['ip', 'host'], ascending=False)
        allNodesDf = allNodesDf.drop_duplicates(subset='ip', keep='first')

        return allNodesDf


    def __getLatestData(self, dateFrom, dateTo):
        df = pd.DataFrame()
        try:
            dt = [dateFrom, dateTo]
            allNodesDf = self.getNodes(dt)
            rows = []
            for item in allNodesDf.to_dict('records'):
                lastRec = qrs.mostRecentMetaRecord(
                    item['ip'], item['ipv6'], dt)
                item['site_index'] = item['site']
                if len(lastRec) > 0:
                    # when there is no site name coming from the main indices or site has
                    # multiple locations like GRIF, get the name from ps_meta
                    if (lastRec['site_meta'] is not None) and (item['site'] is None or item['site'].isin(['GRIF', 'MWT2', 'AGLT2'])):
                        lastRec['site'] = lastRec['site_meta']
                    else:
                        lastRec['site'] = item['site']

                    if item['ipv6'] is not None:
                        lastRec['ipv6'] = item['ipv6']

                    rows.append(lastRec)
                else:
                    rows.append(item)

            df = pd.DataFrame(rows)

            df['last_update'] = df['timestamp'].apply(
                lambda ts: hp.convertTime(ts))
            df['last_update'].fillna(hp.convertTime(dt[1]), inplace=True)
        except Exception as e:
            print(e)
            print(item)
            print(lastRec)
            print()

        return df


    @staticmethod
    def __isHost(val):
        if re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val):
            return True
        return False


    def __updateField(self, ip, field, newRec, oldRec):
        # check if the values are different and verify the new value is not None
        if oldRec[field].values[0] != newRec[field].values[0] and newRec[field].values[0] is not None and newRec[field].values[0] == newRec[field].values[0]:
            if not field == 'host':
                print(
                    f"updating {ip}  {field} {oldRec[field].values[0]} ===> {newRec[field].values[0]}")
                return newRec[field].values[0]
            else:
                if self.__isHost(newRec[field].values[0]):
                    print(
                        f"updating {ip}  {field} {oldRec[field].values[0]} ===> {newRec[field].values[0]}")
                    return newRec[field].values[0]

        return oldRec[field].values[0]


    def __updateDataset(self, df, latest):
        columns = ['ip', 'host', 'site', 'administrator',
                   'email', 'lat', 'lon', 'site_meta', 'site_index']
        merged = latest[columns].merge(
            df[columns], indicator=True, how='outer')
        diff = merged[merged['_merge'] == 'left_only']['ip'].values

        for ip in diff:
            oldRec = df[(df['ip'] == ip)]
            newRec = latest[(latest['ip'] == ip)]

            if len(oldRec) == 0:
                print(f' +++ new row for ip {ip}')
                df = pd.concat([df, newRec])
            else:
                for col in columns[1:]:
                    df.loc[df['ip'] == ip, col] = self.__updateField(
                        ip, col, newRec, oldRec)

        # sort the df by location, so that the non-null/nan go at the bottom,
        # then the missing could be forward-filled based on site name
        df = df.sort_values(['lat', 'lon'])
        df = df.fillna(df[['site', 'lat', 'lon']].groupby('site').ffill())

        return df
