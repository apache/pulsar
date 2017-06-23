---
title: The Pulsar dashboard
---

The Pulsar dashboard is a web application that enables users to monitor current stats for all topics in tabular form.

The dashboard is composed of a *collector* that polls the stats from all the {% popover brokers %} in a Pulsar {% popover instance %} (across multiple {% popover clusters %}) and stores all the information in a Postgres database.

Finally, a Django web app is used to render the collected data.

## Install

The easiest way to use the dashboard is to run it inside a [Docker](https://www.docker.com/products/docker) container. A `Dockerfile` to generate the image is provided.

To generate the Docker image:

```shell
$ docker build -t pulsar-dashboard dashboard
```

To run the dashboard:

```shell
$ MY_SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$MY_SERVICE_URL\
  pulsar-dashboard
```

You need to specify only one service URL for a Pulsar cluster. Internally, the collector will figure out all the existing clusters and the brokers from where it needs to pull the metrics.

Once the Docker container is running, the web dashboard will be accessible at http://localhost, or `http://my.docker.host/`.

### Known issues

Pulsar Authentication is not supported at this point. The collector is not passing any authentication data and will denied access if the Pulsar broker requires authentication.
