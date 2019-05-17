#!/usr/bin/ksh

#Get the script path
__REAL_SCRIPTDIR=$( cd -P -- "$(dirname -- "$(command -v -- "$0")")" && pwd -P )

cd $__REAL_SCRIPTDIR
. ./env.ksh

python cognos.py &
