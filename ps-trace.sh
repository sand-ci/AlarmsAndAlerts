#!/bin/bash
date
python ps-trace.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running routers. Exiting."
    exit $rc
fi
