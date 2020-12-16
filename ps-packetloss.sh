#!/bin/bash
date
# service sendmail start
python3.8 ps-packetloss.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running packetloss. Exiting."
    exit $rc
fi
