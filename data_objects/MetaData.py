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
        print(f'{self.dateFrom} - {self.dateTo} Grab latest data')
        if not dt: dt = hp.GetTimeRanges(self.dateFrom, self.dateTo)
        allNodesDf = qrs.allTestedNodes(dt)
        allNodesDf = self.__removeDuplicates(allNodesDf)
        # in some cases there is one IP having 2 different hostanames
        self.allNodesDf = self.__removeDuplicates(allNodesDf)
        rows = []
        # run a query for each ip because it is not a trivial task (if possible at all) to aggreagate the geolocation fields
        for item in allNodesDf.to_dict('records'):
            lastRec = qrs.mostRecentMetaRecord(item['ip'], item['ipv6'], dt)
            if len(lastRec) > 0:
                lastRec['site_index'] = item['site']
                rows.append(lastRec)
            else: 
                item['site_index'] = item['site']
                rows.append(item)

        columns=['ip', 'timestamp', 'host', 'site', 'administrator', 'email', 'lat', 'lon', 'site_meta', 'site_index']
        df = pd.DataFrame(rows, columns=columns)

        def convertTime(ts):
            if pd.notnull(ts):
                return datetime.utcfromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M')

        df['last_update'] = df['timestamp'].apply(lambda ts: convertTime(ts))

        return df.drop_duplicates()


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
                df.loc[df['ip']==ip, field] = newRec[field].values[0]

                df.loc[df['ip']==ip, 'last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M")

        for ip in diff:
            oldRec = df[(df['ip']==ip)]
            newRec = latest[(latest['ip']==ip)]

            if len(oldRec)==0:
                print(f' ++++++++ new row for ip {ip}')
                # df = df.append(newRec.to_dict('records')[0], ignore_index=True)
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


    def __useOthers2FixField(self, row, fld):
        siteBasedVal = [item for item in self.metaDf[self.metaDf['site']==row.site]['lat'].unique() if item is not None]

        # if there's a value already, return it
        # otherwise if there is a host name, find a similar host and get its value for the field in question
        if row[fld]==row[fld] and row[fld] is not None:
            return row[fld]
            # get first the site name and check if that site has another node and complete data 
            # for that field. If values are the same, it should be safe to replace the missing field.
            # The number of nodes is important when we look at a multi-node site where 
            # for example the location is completely different  
        elif len(siteBasedVal)==1:
            return siteBasedVal[0]

        elif row.host is not None and row.host==row.host:
            # try first with the domain before the second dot from the end
            # Ex. host epgperf.ph.bham.ac.uk - d2 is ac.uk, d3 is bham.ac.uk
            d2 = '.'.join(row.host.split('.')[-2:])
            d3 = '.'.join(row.host.split('.')[-3:])
            possibleVals2 = self.metaDf[self.metaDf['host'].str.endswith(d2, na=False)][fld].tolist()
            possibleVals3 = self.metaDf[self.metaDf['host'].str.endswith(d3, na=False)][fld].tolist()

            if len(possibleVals3)>0 and any(possibleVals3):
#                 print(f'{row.host} look for domain: {d3} found: {fld} {len(possibleVals3)}')
                # take the value only if the possible values share the same site name
                site_list = self.metaDf[self.metaDf['host'].str.endswith(d3, na=False)]['site'].tolist()
                site_list = [item for item in site_list if item is not None]

                if site_list.count(site_list[0]) == len(site_list):
                    return next((item for item in possibleVals3 if item is not None), None)
            elif len(possibleVals2)>0 and any(possibleVals2):
                site_list = self.metaDf[self.metaDf['host'].str.endswith(d2, na=False)]['site'].tolist()
                site_list = [item for item in site_list if item is not None]

                if site_list.count(site_list[0]) == len(site_list):
                    return next((item for item in possibleVals2 if item is not None), None)


    def __fixMissingSites(self, row):
        # The 'site' field is either the site comeing from the measurements, or the one from ps_meta
        # If both are empty, then try to find a similat host name and get it's site name
        if ((row.site_index is None) or (not row.site_index==row.site_index)) and ((row.site_meta is None) or (not row.site_meta==row.site_meta)):
            return self.__useOthers2FixField(row, 'site_index')
        if (row.site_index is None) or (not row.site_index==row.site_index):
            return row.site_meta
        else:
            return row.site_index


    def __removeDuplicates(self, ddf):
        dup = ddf[ddf['ip'].duplicated()]['ip'].values

        def isHost(val):
            if re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", val):
                return True
            return False

        if len(dup)>0:
            idcs2drop = []
            for ip, group in ddf[ddf['ip'].isin(dup)].groupby('ip'):
                
                if len(group)>1:
                    for g in group.host.values:                
                        if isHost(g):
                            print(f'----- IP {ip} for host {g}')
                temp = []
                for idx, gr in group.iterrows():
                    if isHost(gr['host']) == False:
                        temp.append(idx)

                # in case there is no record resolving to host, keep one IP item  
                if len(temp)==len(group):
                    temp.pop()
                idcs2drop.extend(temp)

                if len(group)>2:
                    print(f'More than one hosts assocciated whith IP: {ip}')
                    print(f'{group}')

            print(f'-------- Remove {len(idcs2drop)} duplicates')
            ddf = ddf.drop(idcs2drop)

        return ddf
    
    
    def getMetafromES(self):
        meta = []
        data = scan(hp.es, index='ps_alarms_meta')
        for item in data:
            meta.append(item['_source'])

        if meta:
            return pd.DataFrame(meta)


    def __updateDataset(self):

        metaDf = self.getMetafromES()

        if len(metaDf)>1:
            print('Update meta data')
            self.metaDf = self.__updateMetaData(metaDf, self.__getLatest())
        else:
            # Initially, grab one year of data split into 6 chunks in order to
            # fill in info that may not appear in the most recent data
            print('No data found. Query a year back.')
            dateTo = datetime.strftime(self.now, '%Y-%m-%d %H:%M')
            dateFrom = datetime.strftime(self.now - timedelta(days=365), '%Y-%m-%d %H:%M')
            timeRange = hp.GetTimeRanges(dateFrom, dateTo,10)

            self.metaDf = self.__getLatest([timeRange[0], timeRange[1]])
            for i in range(2,len(timeRange)-1):
                print(f'Period: {timeRange[i]}, {timeRange[i+1]}, data size before update: {len(metaDf)}')
                metaDf = self.__updateMetaData(metaDf, self.__getLatest([timeRange[i], timeRange[i+1]]))
                print(f'Size after update: {len(metaDf)}')
                print('')

        # Finally, try to fix empty fields by searching for similar host names and assign their value
        try:
            self.metaDf['site'] = self.metaDf.apply(lambda row: self.__fixMissingSites(row), axis=1)
            self.metaDf['lat'] = self.metaDf.apply(lambda row: self.__useOthers2FixField(row, 'lat'), axis=1)
            self.metaDf['lon'] = self.metaDf.apply(lambda row: self.__useOthers2FixField(row, 'lon'), axis=1)
            # Some fields' values vary. Keep the rows that seem more complete.
            # Ex: the same IP can get site name 'pic' or 'ifae', keep the one that has less empty fields

        except Exception as e:
            print(traceback.format_exc())
        finally:
            print('Meta data done')