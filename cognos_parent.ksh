#!/usr/bin/ksh

BASE_DIR=/teoco/sa_root_med01
. $BASE_DIR/project/env/env.ksh
export INTEGRATION_DIR=/teoco/sa_root_med01/integration/scripts/
export LOG_DIR=/teoco/sa_root_med01/logs/

python $INTEGRATION_DIR/implementation/cognos/cognos.py &
