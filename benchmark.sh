#!/usr/bin/env bash
while true
do 
    ./Benchmark -c 10.30.95.222:5254 --threads 20 --writes 10000 
    sleep 1
done
