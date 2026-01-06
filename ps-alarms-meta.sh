#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 ps-alarms-meta.py > ps-alarms-meta.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-alarms-meta alarm. Exiting."
    exit $rc
fi