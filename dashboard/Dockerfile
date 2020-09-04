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

FROM python:3.7-stretch

LABEL maintainer="Apache Pulsar <dev@pulsar.apache.org>"

RUN bash -c "echo deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main >> /etc/apt/sources.list.d/pgdg.list"
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update
RUN apt-get -y install postgresql-11 postgresql-contrib libpq-dev sudo nginx supervisor

# Postgres configuration
COPY conf/postgresql.conf /etc/postgresql/11/main/

# Configure nginx and supervisor
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
COPY conf/nginx-app.conf /etc/nginx/sites-available/default
COPY conf/supervisor-app.conf /etc/supervisor/conf.d/

# Copy web-app sources
RUN mkdir /pulsar
COPY . /pulsar/

# Python dependencies
RUN pip install -r /pulsar/requirements.txt

# Collect all static files needed by Django in a
# single place. Needed to run the app outside the
# Django test web server
RUN cd /pulsar/django && ./manage.py collectstatic --no-input

RUN mkdir /data

EXPOSE 80

CMD ["/pulsar/start.sh"]
