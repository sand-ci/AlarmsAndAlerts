from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import pandas as pd
import traceback
import re

import utils.queries as qrs
import utils.helpers as hp

class MetaData(object):

    def __init__(self, dateFrom=None,  dateTo=None):
        if not all([dateFrom, dateTo]):
            self.now = hp.roundTime(datetime.utcnow())
            self.dateFrom = datetime.strftime(self.now - timedelta(days=1), '%Y-%m-%d %H:%M')
            self.dateTo = datetime.strftime(self.now, '%Y-%m-%d %H:%M')
            self.__updateDataset()


    # Grab the latest meta data from ps_meta
    def __getLatest(self, dt=None):
        print(f'{self.dateFrom} - {self.dateTo} Grab latest data', dt)
        if not dt: dt = hp.GetTimeRanges(self.dateFrom, self.dateTo)
        allNodesDf = qrs.allTestedNodes(dt)
        allNodesDf = allNodesDf[(allNodesDf['ip']!='')&~(allNodesDf['ip'].isnull())]
        # in some cases there is one IP having 2 different hostanames
        allNodesDf =self. __removeDuplicates(allNodesDf)
        rows = []
        # run a query for each ip because it is not a trivial task (if possible at all) to aggreagate the geolocation fields
        for item in allNodesDf.sort_values('host',ascending=False).to_dict('records'):
            lastRec = qrs.mostRecentMetaRecord(item['ip'], item['ipv6'], dt)
            if len(lastRec) > 0:
                rows.append(lastRec)
            else: 
                item['site_index'] = item['site']
                rows.append(item)

        columns=['ip', 'timestamp', 'host', 'site', 'administrator', 'email', 'lat', 'lon', 'site_meta', 'site_index']
        df = pd.DataFrame(rows, columns=columns)

        df = df.drop_duplicates()

        df['last_update'] = df['timestamp'].apply(lambda ts: self.convertTime(ts))
        df['last_update'].fillna(self.convertTime(dt[1]), inplace=True)

        return df


    def convertTime(self, ts):
        if pd.notnull(ts):
            return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M')


    def isHost(self, val):
        if re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val):
            return True
        return False


    def __updateMetaData(self, df, latest):
        columns = ['ip', 'host', 'site', 'administrator', 'email', 'lat','lon', 'site_meta', 'site_index']
        # extract the differencies only
        merged = latest[columns].merge(df[columns], indicator=True, how='outer')
        diff = merged[merged['_merge'] == 'left_only']['ip'].values

        # Compare IP by IP and update/add the new data if not null
        def changeValue(ip, field, newRec):
            field1 = field if field != 'site_meta' else 'site'

            # check if value is not nan 
            if newRec[field].values[0] == newRec[field].values[0]:
                print(f"updating {ip}  {field} {df[(df['ip']==ip)][field].values[0]} ===> {newRec[field].values[0]}")
                if not field == 'host':
                    df.loc[df['ip']==ip, field] = newRec[field].values[0]
                    df.loc[df['ip']==ip, 'last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M")
                else:
                    if self.isHost(newRec[field].values[0]):
                        df.loc[df['ip']==ip, field] = newRec[field].values[0]
                        df.loc[df['ip']==ip, 'last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M")

        for ip in diff:
            oldRec = df[(df['ip']==ip)]
            newRec = latest[(latest['ip']==ip)]

            if len(oldRec)==0:
                # print(f' ++++++++ new row for ip {ip}')
                df = pd.concat([df, newRec])
            else:
                if oldRec['host'].values[0]!=newRec['host'].values[0] and newRec['host'].values[0] is not None:
                    changeValue(ip,'host',newRec)

                if oldRec['site_meta'].values[0]!=newRec['site_meta'].values[0] and newRec['site_meta'].values[0] is not None:
                    changeValue(ip,'site_meta',newRec)

                if oldRec['site_index'].values[0]!=newRec['site_index'].values[0] and newRec['site_index'].values[0] is not None:
                    changeValue(ip,'site_index',newRec)

                if oldRec['administrator'].values[0]!=newRec['administrator'].values[0] and newRec['administrator'].values[0] is not None:
                    changeValue(ip,'administrator',newRec)

                if oldRec['email'].values[0]!=newRec['email'].values[0] and newRec['email'].values[0] is not None:
                    changeValue(ip,'email',newRec)

                if oldRec['lat'].values[0]!=newRec['lat'].values[0] and newRec['lat'].values[0] is not None:
                    changeValue(ip,'lat',newRec)

                if oldRec['lon'].values[0]!=newRec['lon'].values[0] and newRec['lon'].values[0] is not None:
                    changeValue(ip,'lon',newRec)
                    
        return df


    def __fixMissingSites(self, metaDf):
        # The 'site' field is either the site comeing from the measurements, or the one from ps_meta
        # If both are empty, then try to find a similat host name and get it's site name
        metaDf['site'].fillna(metaDf['site_index'], inplace=True)
        metaDf['site'].fillna(metaDf['site_meta'], inplace=True)

        return metaDf.drop_duplicates()


    def __removeDuplicates(self, ddf):
        dup = ddf[ddf['ip'].duplicated()]['ip'].values

        if len(dup)>0:
            idcs2drop = []
            for ip, group in ddf[ddf['ip'].isin(dup)].groupby('ip'):
                
                site = set([s for s in group.site.values if s is not None])
                hosts = set(group.host.values)
                boolHosts = [self.isHost(h) for h in hosts]

                if not all(boolHosts):
                    if any(boolHosts):
                        temp = []
                        for idx, gr in group.iterrows():
                            if self.isHost(gr['host']) == False:
                                # prepare to drop rows where host was not resolved
                                temp.append(idx)
                        idcs2drop.extend(temp)

                # Fix site name = None, replace with known value
                if len(site) >= 1:
                    ddf.loc[ddf.ip == ip, 'site'] = list(site)[0]
                    if len(site)>1:
                        print(f'More than one site assocciated whith IP: {ip}', site)
                        print(f'Assigning the first - {list(site)[0]}')


            print(f'-------- Remove {len(idcs2drop)} duplicates')
            ddf = ddf.drop(idcs2drop)
            ddf.drop_duplicates(inplace=True)

        return ddf


    def getMetafromES(self):
        meta = []
        data = scan(hp.es, index='ps_alarms_meta')
        for item in data:
            meta.append(item['_source'])

        if meta:
            return pd.DataFrame(meta)


    def __updateDataset(self):

        self.metaDf = self.getMetafromES()

        if len(self.metaDf)>1:
            print('Update meta data')
            self.metaDf = self.__updateMetaData(self.metaDf, self.__getLatest())
        else:
            # Initially, grab one year of data split into 6 chunks in order to
            # fill in info that may not appear in the most recent data
            print('No data found. Query a year back.')
            dateTo = datetime.strftime(self.now, '%Y-%m-%d %H:%M')
            dateFrom = datetime.strftime(self.now - timedelta(days=365), '%Y-%m-%d %H:%M')
            timeRange = hp.GetTimeRanges(dateFrom, dateTo,10)

            self.metaDf = self.__getLatest([timeRange[0], timeRange[1]])
            for i in range(2,len(timeRange)-1):
                print(f'Period: {timeRange[i]}, {timeRange[i+1]}, data size before update: {len(self.metaDf)}')
                self.metaDf = self.__updateMetaData(self.metaDf, self.__getLatest([timeRange[i], timeRange[i+1]]))
                print(f'Size after update: {len(self.metaDf)}')
                print()


            # self.metaDf.loc[self.metaDf.site == 'CERN-PROD', 'lat'] = 46.2416566
            # self.metaDf.loc[self.metaDf.site == 'CERN-PROD', 'lon'] = 6.0468415

        # Finally, try to fix empty fields by searching for similar host names and assign their value
        try:
            self.metaDf = self.__fixMissingSites(self.metaDf)
            self.metaDf = self.metaDf.fillna(self.metaDf[['site','lat','lon']].groupby('site').ffill())
            self.metaDf = self.metaDf.drop_duplicates(subset='ip', keep="last")

            # remove all >1500 nodes for which there is no meaningful info
            self.metaDf.fillna('', inplace=True)
            toRemoveIds = self.metaDf[~(self.metaDf['lat'].astype(bool))&~(self.metaDf['lon'].astype(bool))\
                                &~(self.metaDf['administrator'].astype(bool))&~(self.metaDf['site_index'].astype(bool))\
                                &~(self.metaDf['site_meta'].astype(bool))&~(self.metaDf['email'].astype(bool))].index.values
            self.metaDf = self.metaDf[~self.metaDf.index.isin(toRemoveIds)]

        except Exception as e:
            print(traceback.format_exc())
        finally:
            print('Meta data done')