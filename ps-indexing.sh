#!/bin/bash
date
python ps-indexing.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
fi
echo 'starting Nebraska'
python ps-indexing.nebraska.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking ps indexing. Exiting."
fi