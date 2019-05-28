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

FROM apachepulsar/pulsar-all:latest as pulsar

# Restart from
FROM openjdk:8-jdk

COPY --from=pulsar /pulsar /pulsar

RUN apt-get update
RUN apt-get -y install python2.7 python-pip postgresql-9.6 sudo nginx supervisor

# Python dependencies
RUN pip2 -qq install uwsgi 'Django<2.0' 'psycopg2==2.7.7' pytz requests

# Postgres configuration
COPY conf/postgresql.conf /etc/postgresql/9.6/main/

# Configure nginx and supervisor
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
COPY conf/nginx-app.conf /etc/nginx/sites-available/default
COPY conf/supervisor-app.conf /etc/supervisor/conf.d/

# Copy web-app sources
COPY . /pulsar/

# Setup database and create tables
RUN sudo -u postgres /etc/init.d/postgresql start && \
    sudo -u postgres psql --command "CREATE USER docker WITH PASSWORD 'docker';" && \
    sudo -u postgres createdb -O docker pulsar_dashboard && \
    cd /pulsar/django && \
    ./manage.py migrate && \
    sudo -u postgres /etc/init.d/postgresql stop

# Collect all static files needed by Django in a
# single place. Needed to run the app outside the
# Django test web server
RUN cd /pulsar/django && ./manage.py collectstatic --no-input

ENV SERVICE_URL http://127.0.0.1:8080
EXPOSE 80

CMD ["supervisord", "-n"]
