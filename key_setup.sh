#!/bin/bash

#######################################################
# Key Setup for Loggly
# see also 
# - Loggly configuration in syslog_configurer.py
# - https://community.loggly.com/customer/portal/articles/1225987-syslog-ng-configuration#tls
#######################################################


# TODO error handling

KEY_DIR=/etc/syslog-ng/keys/ca.d

mkdir -p $KEY_DIR

pushd .
cd $KEY_DIR

wget https://logdog.loggly.com/media/loggly.com.crt
wget https://certs.starfieldtech.com/repository/sf_bundle.crt

cat {sf_bundle.crt,loggly.com.crt} > loggly_full.crt
rm  {sf_bundle.crt,loggly.com.crt}

popd