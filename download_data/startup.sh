#!/bin/bash

LAST_RUN_LOG=/home/ubuntu/logs/last_run.log
if (( $(grep $(date +%F) $LAST_RUN_LOG | wc -l) == 0 )); then
  # Run only if was not run today
  date '+%F %T' > $LAST_RUN_LOG
  PYTHON='/home/ubuntu/Anaconda3/envs/air-download-data/bin/python'
  SCRIPT='/home/ubuntu/air-pollution/data_collecting.py'
  LOG='/home/ubuntu/logs/download_data.log'
  $PYTHON -u $SCRIPT |& tee -a $LOG
  sudo shutdown -P -t 120
fi
