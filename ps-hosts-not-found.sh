#!/bin/bash
date
python ps-hosts-not-found.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running hosts not found. Exiting."
    exit $rc
fi
