#!/bin/bash
date
python3.8 ps-throughput.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running throughput. Exiting."
    exit $rc
fi
