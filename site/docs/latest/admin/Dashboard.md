---
title: The Pulsar dashboard
---

The Pulsar dashboard is a web application that enables users to monitor current stats for all {% popover topics %} in tabular form.

The dashboard is a data collector that polls stats from all the {% popover brokers %} in a Pulsar {% popover instance %} (across multiple {% popover clusters %}) and stores all the information in a [PostgreSQL](https://www.postgresql.org/) database.

A [Django](https://www.djangoproject.com) web app is used to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container. A [`Dockerfile`]({{ site.pulsar_repo }}/dashboard/Dockerfile) to generate the image is provided.

To generate the Docker image:

```shell
$ docker build -t pulsar-dashboard dashboard
```

To run the dashboard:

```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  pulsar-dashboard
```

You need to specify only one service URL for a Pulsar cluster. Internally, the collector will figure out all the existing clusters and the brokers from where it needs to pull the metrics. If you're connecting the dashboard to Pulsar running in {% popover standalone %} mode, the URL will be `http://localhost:8080` by default.

Once the Docker container is running, the web dashboard will be accessible via `localhost` or whichever host is being used by Docker.

### Known issues

Pulsar [authentication](../../admin/Authz#authentication-providers) is not supported at this point. The dashboard's data collector does not pass any authentication-related data and will be denied access if the Pulsar broker requires authentication.
