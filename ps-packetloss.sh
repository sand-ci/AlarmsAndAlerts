#!/bin/bash
date
python ps-packetloss.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running packetloss. Exiting."
    exit $rc
fi
