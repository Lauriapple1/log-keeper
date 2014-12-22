FROM ubuntu

# Mount directories for app (Docker container) related data. Serves as input data for log-keeper.
VOLUME /app

# Mount directory for Docker host related data. Serves as input data for log-keeper.
VOLUME /host

# install syslog-ng as log sync tool + incron to watch newly hooked applications
RUN apt-get update
RUN apt-get install -y syslog-ng syslog-ng-core 
RUN apt-get install -y incron
RUN apt-get install -y wget

# Clean up system
RUN apt-get clean
RUN rm -rf /tmp/* /var/tmp/*
RUN rm -rf /var/lib/apt/lists/*

# stop service (we will run it later via CMD)
RUN service syslog-ng stop

# Replace the system() source because inside Docker we can't access /proc/kmsg.
# https://groups.google.com/forum/#!topic/docker-user/446yoB0Vx6w
RUN sed -i -E 's/^(\s*)system\(\);/\1unix-stream("\/dev\/log");/' /etc/syslog-ng/syslog-ng.conf

# add syslog-ng configuration
ADD host.conf_custom /etc/syslog-ng/conf.d/host.conf_custom

# Configure trigger for creating new syslog-ng config for each new app hooking in to /app
RUN rm /etc/incron.allow
ADD syslog_configurer.py /etc/syslog_configurer.py
ADD incron.conf /etc/log_monitor_incron.conf
RUN incrontab /etc/log_monitor_incron.conf

# key setup for Loggy
ADD key_setup.sh /etc/key_setup.sh
RUN chmod a+x /etc/key_setup.sh
RUN /etc/key_setup.sh

# perform final initialization steps and syslog-ng in foreground mode
ADD run.sh /etc/run.sh
RUN chmod a+x /etc/run.sh
CMD ["/etc/run.sh"]
