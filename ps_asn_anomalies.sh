#!/bin/bash
date
python ps_asn_anomalies.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running ps-alarms-meta. Exiting."
    exit $rc
fi
