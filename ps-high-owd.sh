#!/bin/bash

date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 ps-high-owd.py > ps-high-owd.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-high-owd alarm. Exiting."
    exit $rc
fi