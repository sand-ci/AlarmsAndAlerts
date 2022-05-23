# #
# Downloads the Research & Education Autonomous System Numbers (ASNs)
# and stores the information in ps_asns index in ES
# #

import requests
from elasticsearch.helpers import parallel_bulk

import utils.helpers as hp


def loadASNs():
    response = requests.get("https://bgp.nsrc.org/REN/USP/data-used-autnums", verify=False)
    content = response.text.strip()
    lines = content.split('\n')

    asnDict = {}
    try:
        for l in lines:
            # print(l)
            row = l.strip()
            row = row.split(' ', 1)
            if not row[1] in ["UNALLOCATED", "UNASSIGNED"]:
                asnDict[int(row[0])] = row[1]

        return asnDict
    except Exception as e:
        print('Issue wtih:', e, row)


def genData(asnInfo):
    for asn, owner in asnInfo.items():
        yield {
            "_index": "ps_asns",
            "_id": asn,
            "owner": owner,
        }


asnInfo = loadASNs()
print(f'Number of ASNs to store: {len(asnInfo)}')

for success, info in parallel_bulk(hp.es, genData(asnInfo)):
    if not success:
        print('A document failed:', info)
