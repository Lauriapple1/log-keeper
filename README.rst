==========
Log Keeper
==========

`Log Keeper` handles the log management for hosts running a Docker daemon. It runs a `syslog-ng` daemon transferring
the log files from the Docker host and of other Docker containers referred to as `applications` in this context. 

When `Log Keeper` is started it looks for already running `applications` in its `/app` mount (see description below)
and creates corresponding `syslog-ng` configurations. In addition, it monitors `/app` for new applications showing up
in order to configure `syslog-ng` to take care for those logs as well.  *NOTE:* These mechanisms work only if you follow
the steps specified below:


Running
=======
.. code-block:: bash

    $ mkdir /var/app_log_share
    $ docker run  -d -e "loggly_auth_token=<your Loggly AUTH token>"  -v /var/log:/host/log -v /var/app_log_share:/app  zalando/log_keeper:0.1

Example:

.. code-block:: bash

    $ mkdir /var/app_log_share
    $ docker run  -d -e "loggly_auth_token=MY_LOGGY_AUTH_TOKEN" -v /var/log:/host/log -v /var/app_log_share:/app zalando/log_keeper:0.1

App Deployment
==============

In order to provide your log files to `Log Keeper` you need to create an application folder in your `app log share`
with format `<app-name>-<app-version>-<uuid>`. `Log Keeper` monitors the `/app` mount for each appearing application 
folder and creates a new `syslog-ng` configuration.

.. code-block:: bash

    $ app_id=$(uuid | sed  's/-/_/g')
    $ mdkir -p /var/app_log_share/<app-name>-<app-version>-$app_id/log
    $ docker run -d -v /var/app_log_share/<app-name>-<app-version>-$app_id/log:/var/log <your image>

Example:

.. code-block:: bash

    $ app_id=$(uuid | sed  's/-/_/g')
    $ mdkir -p /var/app_log_share/logmeister-0.1-$app_id/log
    $ docker run -d -v /var/app_log_share/logmeister-0.1-$app_id/log:/var/log zalando/logmeister:0.1



