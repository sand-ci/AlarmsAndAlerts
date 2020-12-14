import time
import datetime
import pandas as pd

import utils.queries as qrs
import utils.helpers as hp
from utils.helpers import timer
from data_objects.NodesMetaData import NodesMetaData



class PrtoblematicPairsDataLoader(object):

    def __init__(self, idx):
        defaultDT = hp.defaultTimeRange()
        self.dateFrom,  self.dateTo = defaultDT[0], defaultDT[1]
        self.df = self.markPairs(idx)


    @timer
    def loadData(self, idx):
        print('loadData...',idx)
        data = []
        intv = int(hp.CalcMinutes4Period(self.dateFrom, self.dateTo)/60)
        time_list = hp.GetTimeRanges(self.dateFrom, self.dateTo, intv)
        for i in range(len(time_list)-1):
            data.extend(qrs.query4Avg(idx, time_list[i], time_list[i+1]))

        return data


    @timer
    def getPercentageMeasuresDone(self, grouped, tempdf):
        measures_done = tempdf.groupby('hash').agg({'doc_count':'sum'})
        def findRatio(row, total_minutes):
            if pd.isna(row['doc_count']):
                count = '0'
            else: count = str(round((row['doc_count']/total_minutes)*100))+'%'
            return count

        one_test_per_min = hp.CalcMinutes4Period(self.dateFrom, self.dateTo)
        measures_done['tests_done'] = measures_done.apply(lambda x: findRatio(x, one_test_per_min), axis=1)
        grouped = pd.merge(grouped, measures_done, on='hash', how='left')

        return grouped


    @timer
    def markPairs(self, idx):
        df = pd.DataFrame()

        tempdf = pd.DataFrame(self.loadData(idx))
        grouped = tempdf.groupby(['src', 'dest', 'hash', 'src_host', 'dest_host', 'src_site', 'dest_site']).agg({'value': lambda x: x.mean(skipna=False)}, axis=1).reset_index()
        
        grouped = self.fixMissingMetadata(grouped, idx)

        # calculate the percentage of measures based on the assumption that ideally measures are done once every minute
        grouped = self.getPercentageMeasuresDone(grouped, tempdf)

        if idx == 'ps_packetloss':
            # set value to 0 - we consider there is no issue bellow 1% loss
            # set value to 1 - the pair is marked problematic between 1% and 100% loss
            # set value to 2 - the pair shows 100% loss
            def setFlag(x):
                if x>=0 and x<0.02:
                    return 0
                elif x>=0.02 and x<1:
                    return 1
                elif x==1:
                    return 2
                return 'something is wrong'


            grouped['flag'] = grouped['value'].apply(lambda val: setFlag(val))


        df = df.append(grouped, ignore_index=True)
        df.rename(columns={'value':'avg_value'}, inplace=True)
        
        print(f'Total number of hashes: {len(df)}')

        return df


    @timer
    def fixMissingMetadata(self, probdf, idx):
        metadf = NodesMetaData(idx, self.dateFrom, self.dateTo).df
        df1 = pd.merge(metadf[['host', 'ip', 'site']], probdf[['src', 'hash']], left_on='ip', right_on='src', how='right')
        df2 = pd.merge(metadf[['host', 'ip', 'site']], probdf[['dest', 'hash']], left_on='ip', right_on='dest', how='right')
        df = pd.merge(df1, df2, on=['hash'], suffixes=('_src', '_dest'), how='inner')
        df = df[df.duplicated(subset=['hash'])==False]

        df = df.drop(columns=['ip_src', 'ip_dest'])
        df = pd.merge(probdf, df, on=['hash', 'src', 'dest'], how='left')

        return df