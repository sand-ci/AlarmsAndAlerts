#!/bin/bash
date
python ps-asns.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running asns. Exiting."
    exit $rc
fi
