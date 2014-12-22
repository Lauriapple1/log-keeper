#!/bin/bash

# configure syslog-ng for log files of the host and already running applications
python3 /etc/syslog_configurer.py --new_main $loggly_auth_token
status=$?
if [ $status -ne 0 ]
then
  echo "could not configure syslog-ng: " $status
  exit -1
fi
 
# start incrond daemon to watch /app mount
# see incron.conf for further details
incrond &

# --no-caps to avoid:
# syslog-ng: Error setting capabilities, capability management disabled; error='Operation not permitted'
# http://serverfault.com/questions/524518/error-setting-capabilities-capability-management-disabled#
# https://github.com/dockerbase/syslog-ng/
/usr/sbin/syslog-ng --foreground --no-caps
