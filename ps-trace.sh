#!/bin/bash
date
python routers.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running routers. Exiting."
    exit $rc
fi
