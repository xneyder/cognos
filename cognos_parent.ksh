#!/usr/bin/ksh

BASE_DIR=/teoco/sa_root_med01
. $BASE_DIR/project/env/env.ksh
export INTEGRATION_DIR=/teoco/sa_root_med01/integration/scripts/
export LOG_DIR=/teoco/sa_root_med01/logs/
export DB_USER=''
export DB_PASSWORD=''
export ORACLE_SID=''
export DB_HOST=''


python $INTEGRATION_DIR/implementation/cognos/cognos.py &
