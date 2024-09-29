#!/bin/bash
date
python ps-host-unresolvable.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running throughput. Exiting."
    exit $rc
fi
