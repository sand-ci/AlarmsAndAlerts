#!/bin/bash
date
python ps-alarms-meta.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-alarms-meta. Exiting."
    exit $rc
fi
