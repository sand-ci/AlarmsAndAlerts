#!/bin/bash
date
python ps-trace_path_changed.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running trace path change detection. Exiting."
    exit $rc
fi
