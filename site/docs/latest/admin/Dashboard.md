---
title: The Pulsar dashboard
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

The Pulsar dashboard is a web application that enables users to monitor current stats for all {% popover topics %} in tabular form.

The dashboard is a data collector that polls stats from all the {% popover brokers %} in a Pulsar {% popover instance %} (across multiple {% popover clusters %}) and stores all the information in a [PostgreSQL](https://www.postgresql.org/) database.

A [Django](https://www.djangoproject.com) web app is used to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container. A [`Dockerfile`]({{ site.pulsar_repo }}/dashboard/Dockerfile) to generate the image is provided.

To generate the Docker image:

```shell
$ docker build -t apachepulsar/pulsar-dashboard dashboard
```

To run the dashboard:

```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  apachepulsar/pulsar-dashboard
```

You need to specify only one service URL for a Pulsar cluster. Internally, the collector will figure out all the existing clusters and the brokers from where it needs to pull the metrics. If you're connecting the dashboard to Pulsar running in {% popover standalone %} mode, the URL will be `http://localhost:8080` by default.

Once the Docker container is running, the web dashboard will be accessible via `localhost` or whichever host is being used by Docker.

{% include admonition.html type="warning" content="The `SERVICE_URL` that the dashboard uses needs to be reachable from inside the Docker container" %}

If the Pulsar service is running in standalone mode in `localhost`, the `SERVICE_URL` would have to
be the IP of the machine.

Similarly, given the Pulsar standalone advertises itself with localhost by default, we need to
explicitely set the advertise address to the host IP. For example:

```shell
$ bin/pulsar standalone --advertised-address 1.2.3.4
```

### Known issues

Pulsar [authentication](../../security/overview#authentication-providers) is not supported at this point. The dashboard's data collector does not pass any authentication-related data and will be denied access if the Pulsar broker requires authentication.
