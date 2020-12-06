import requests
import json
host = 'https://aaas.atlas-ml.org/alarm/'


class alarms:
    def __init__(self, category, subcategory, event):
        self.category = category
        self.subcategory = subcategory
        self.event = event

    def addAlarm(self, body, tags=[]):
        js = {
            "category": self.category,
            "subcategory": self.subcategory,
            "event": self.event,
            "body": body,
            "tags": tags
        }
        res = requests.post(host, json=js)
        print(res)
        print(res.status_code)
