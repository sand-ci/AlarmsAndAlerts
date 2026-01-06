#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 ps-asns.py > ps-asns.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-asns alarm. Exiting."
    exit $rc
fi