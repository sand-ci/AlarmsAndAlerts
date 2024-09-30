#!/bin/bash
date
python ps-host-unresolvable.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running host-unresolvable. Exiting."
    exit $rc
fi
