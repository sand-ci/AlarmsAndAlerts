#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 ps-asn-anomalies.py > ps-asn-anomalies.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-asn-anomalies alarm. Exiting."
    exit $rc
fi