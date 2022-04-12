#!/bin/bash
date
python3.8 ps-indexing.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
fi
echo 'starting Nebraska'
python3.8 ps-indexing.nebraska.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
fi