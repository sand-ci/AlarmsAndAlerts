#!/bin/bash

date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
python3 routers.py > routers.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running routers alarm. Exiting."
    exit $rc
fi