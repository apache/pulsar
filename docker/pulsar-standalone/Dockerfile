#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Using files from pulsar-all docker image for pulsar
FROM apachepulsar/pulsar-all:latest as pulsar

# Using files from pulsar-dashboard for the dashboard
FROM apachepulsar/pulsar-dashboard:latest as dashboard

# Restart from
FROM openjdk:8-jdk

# Note that the libpq-dev package is needed here in order to install
# the required python psycopg2 package (for postgresql) later
RUN apt-get update
RUN apt-get -y install python2.7 python-pip postgresql sudo nginx supervisor libpq-dev

# Postgres configuration
COPY --from=dashboard /etc/postgresql/11/main/postgresql.conf /etc/postgresql/11/main/postgresql.conf

# Configure supervisor
COPY --from=dashboard /etc/supervisor/conf.d/supervisor-app.conf /etc/supervisor/conf.d/supervisor-app.conf

# Add pulsar to supervisord. Redirect logs to stdout
RUN echo "[program:pulsar]\n\
command = /pulsar/bin/pulsar standalone\n\
stdout_logfile=/dev/fd/1\n\
stdout_logfile_maxbytes=0\n\
redirect_stderr=true" >> /etc/supervisor/conf.d/supervisor-app.conf 

# Configure nginx
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
COPY --from=dashboard /etc/nginx/sites-available/default /etc/nginx/sites-available/default

# Copy pulsar files from pulsar-all
COPY --from=pulsar /pulsar /pulsar

# Copy web-app sources
COPY . /pulsar/

# Copy dashboard files from pulsar-dashboard
COPY --from=dashboard /pulsar/django /pulsar/django
COPY --from=dashboard /pulsar/requirements.txt /pulsar/django
COPY --from=dashboard /pulsar/conf/* /pulsar/conf/

# Python dependencies
RUN pip2 install -r /pulsar/django/requirements.txt

# Copy web-app sources
# Setup database and create tables
COPY --from=dashboard /pulsar/init-postgres.sh /pulsar/django/init-postgres.sh
RUN mkdir /data
RUN /pulsar/django/init-postgres.sh
RUN sudo -u postgres /etc/init.d/postgresql stop

# Collect all static files needed by Django in a
# single place. Needed to run the app outside the
# Django test web server
RUN cd /pulsar/django && ./manage.py collectstatic --no-input

ENV SERVICE_URL http://127.0.0.1:8080
EXPOSE 80

CMD ["supervisord", "-n"]
