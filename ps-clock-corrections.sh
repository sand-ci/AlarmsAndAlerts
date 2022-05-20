#!/bin/bash
date
python ps-clock-corrections.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running owd clock corrections. Exiting."
    exit $rc
fi
