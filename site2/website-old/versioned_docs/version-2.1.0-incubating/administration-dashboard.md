---
id: version-2.1.0-incubating-administration-dashboard
title: The Pulsar dashboard
sidebar_label: Dashboard
original_id: administration-dashboard
---

The Pulsar dashboard is a web application that enables users to monitor current stats for all [topics](reference-terminology.md#topic) in tabular form.

The dashboard is a data collector that polls stats from all the brokers in a Pulsar instance (across multiple clusters) and stores all the information in a [PostgreSQL](https://www.postgresql.org/) database.

A [Django](https://www.djangoproject.com) web app is used to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container. A {@inject: github:`Dockerfile`:/dashboard/Dockerfile} to generate the image is provided.

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

You need to specify only one service URL for a Pulsar cluster. Internally, the collector will figure out all the existing clusters and the brokers from where it needs to pull the metrics. If you're connecting the dashboard to Pulsar running in standalone mode, the URL will be `http://<broker-ip>:8080` by default. `<broker-ip>` is the ip address or hostname of the machine running Pulsar standalone. The ip address or hostname should be accessible from the docker instance running dashboard.

Once the Docker container is running, the web dashboard will be accessible via `localhost` or whichever host is being used by Docker.

> The `SERVICE_URL` that the dashboard uses needs to be reachable from inside the Docker container

If the Pulsar service is running in standalone mode in `localhost`, the `SERVICE_URL` would have to
be the IP of the machine.

Similarly, given the Pulsar standalone advertises itself with localhost by default, we need to
explicitly set the advertise address to the host IP. For example:

```shell
$ bin/pulsar standalone --advertised-address 1.2.3.4
```

### Known issues

Pulsar [authentication](security-overview.md#authentication-providers) is not supported at this point. The dashboard's data collector does not pass any authentication-related data and will be denied access if the Pulsar broker requires authentication.
