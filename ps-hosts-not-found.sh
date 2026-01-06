#!/bin/bash
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 ps-hosts-not-found.py > ps-hosts-not-found.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-hosts-not-found alarm. Exiting."
    exit $rc
fi