import requests
host = 'https://aaas.atlas-ml.org/alarm/'


class alarms:
    def __init__(self, category, subcategory, event):
        self.category = category
        self.subcategory = subcategory
        self.event = event

    def addAlarm(self, body, tags=[], level=None, source=None, details=None):
        js = {
            "category": self.category,
            "subcategory": self.subcategory,
            "event": self.event,
            "body": body
        }
        if tags:
            js['tags'] = tags
        if level:
            js['level'] = level
        if source:
            js['source'] = source
        if details:
            js['details'] = details
        res = requests.post(host, json=js)
        print(res)
        print(res.status_code)
