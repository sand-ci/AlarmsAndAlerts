#!/bin/bash
date
python ps-site-report.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running site reports. Exiting."
    exit $rc
fi
