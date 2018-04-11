#!/bin/sh
darkcyan='\033[0;31m'
normal=$'\e[0m'

if [ $ENVIRONMENT ]; then
    ENVIRONMENT="[$ENVIRONMENT]"
fi

echo -e $darkcyan+ $ENVIRONMENT $@$normal

LOG=`$@ 2>&1`
RETVAL=$?
if [ $RETVAL -gt 0 ] || [ $VERBOSE ] ; then
    echo "$LOG"
fi

exit $RETVAL
