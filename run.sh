#!/bin/bash

#python ./src/process_log.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json

#python ./src/process_log.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json

python ./src/anomaly_detection.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json ./log_output/largest_networks.dat 

#python ./src/anomaly_detection.py ./sample_dataset/batch_log.json ./sample_dataset/stream_log.json ./log_output/flagged_purchases.json  ./log_output/largest_networks.dat 
