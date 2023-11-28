#!/bin/bash
date
python ps_alarms_meta.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running asns. Exiting."
    exit $rc
fi
