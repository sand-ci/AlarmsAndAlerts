# ps-site-report - This alarm creates weekly reports per each site. 
#                  It extracts from Elasticsearch all unique site  
#                  names and creates an alarm that serves as a notification
#                  that provides user with the screen of the analytics 
#                  page with the list of alarms, their distribution 
#                  throughout the week, etc, and send the link to this page.
#                    
# Author: Yana Holoborodko
# Copyright 2025
import warnings

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)


from utils.queries import getMetaData
from alarms import alarms
import hashlib
from datetime import datetime, timedelta



def create_alarms():
    df_meta = getMetaData()
    combined_sites = df_meta['site'].where(df_meta['site'].notna(), 
                        df_meta['netsite'].where(df_meta['netsite'].notna(), 
                        df_meta['site_meta']))

    unique_sites = list(set(combined_sites.dropna().unique()))
    now = datetime.now()
    fromDayDate = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    fromDay = fromDayDate.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    toDayDate = now.replace(hour=23, minute=59, second=59, microsecond=00000)
    toDay = toDayDate.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    
    for site in unique_sites:   
        alarmOnSite = alarms('Networking', 'Report', "weekly site report")
        html_msg = (
                    f"<h2>Site Report: {site}</h2>"
                    f"<p><strong>Reporting Period:</strong> {fromDayDate.strftime('%d %B %Y')} – {toDayDate.strftime('%d %B %Y')}</p>"
                    f"<p><strong>Link:</strong> <a href='https://ps-dash.uc.ssl-hep.org/site_report/{site}'>https://ps-dash.uc.ssl-hep.org/site_report/{site}</a></p>"
                )
        
        doc = {'from': fromDay,
               'to': toDay,
               'tag': site,
               'report_link': f"https://ps-dash.uc.ssl-hep.org/site_report/{site}",
               'message': f"""
                        <html>
                        <body>
                            <p style="font-family:sans-serif; font-size:16px;">{html_msg}</p>
                        </body>
                        </html>
                        """,
               }
        toHash = ','.join([site, fromDay, toDay, doc['report_link']])
        doc['alarm_id'] = hashlib.sha224(toHash.encode('utf-8')).hexdigest()
        alarmOnSite.addAlarm(body=f'Site report for {site}', tags=[site], source=doc)
  
if __name__ == '__main__':
    create_alarms()
