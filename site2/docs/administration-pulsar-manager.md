---
id: administration-pulsar-manager
title: The Pulsar Manager
sidebar_label: Pulsar Manager
---

Pulsar Manager is a web-based GUI management and monitoring tool that helps administrators and users manage and monitor tenants, namespaces, topics, subscriptions, brokers, clusters, and so on, and supports dynamic configuration of multiple environments.

## Install

The easiest way to use the pulsar manager is to run it inside a [Docker](https://www.docker.com/products/docker) container.


```
docker pull apachepulsar/pulsar-manager:v0.1.0
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.0.104 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -v $PWD:/data apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* REDIRECT_HOST: the IP address of the front-end server.

* REDIRECT_PORT: the port of the front-end server.

* DRIVER_CLASS_NAME: the driver class name of PostgreSQL.

* URL: the url of PostgreSQL jdbc, example: jdbc:postgresql://127.0.0.1:5432/pulsar_manager.

* USERNAME: the username of PostgreSQL.

* PASSWORD: the password of PostgreSQL.

* LOG_LEVEL: level of log.

You can find the in the [docker](https://github.com/apache/pulsar-manager/tree/master/docker) directory and build an image from scratch as well:

```
git clone https://github.com/apache/pulsar-manager
cd pulsar-manager
./gradlew build -x test
cd front-end
npm install --save
npm run build:prod
cd ..
docker build -f docker/Dockerfile --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` --build-arg VCS_REF=`git rev-parse --short HEAD` --build-arg VERSION=`git rev-parse --short HEAD` -t apachepulsar/pulsar-manager .
```

### Use custom databases

If you have a large amount of data, you can use a custom database. The following is an example of PostgreSQL.   

1. Initialize database and table structures using [file](https://github.com/apache/pulsar-manager/tree/master/src/main/resources/META-INF/sql/postgresql-schema.sql).

2. Modify the [configuration file](https://github.com/apache/pulsar-manager/blob/master/src/main/resources/application.properties) and add PostgreSQL configuration

```
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://127.0.0.1:5432/pulsar_manager
spring.datasource.username=postgres
spring.datasource.password=postgres
```

3. Compile to generate a new executable jar package

```
./gradlew -x build -x test
```

### Enable JWT Auth

If you want to turn on JWT authentication, configure the following parameters:

* backend.jwt.token  token for the superuser. You need to configure this parameter during cluster initialization.
* jwt.broker.token.mode  Two modes of generating token, SECRET and PRIVATE.
* jwt.broker.public.key Configure this option if you are using the PRIVATE mode.
* jwt.broker.private.key Configure this option if you are using the PRIVATE mode.
* jwt.broker.secret.key Configure this option if you are using the SECRET mode.

For more information, see [Apache Pulsar](http://pulsar.apache.org/docs/en/security-token-admin/)

* Method 1: Use command-line tool

```
./build/distributions/pulsar-manager/bin/pulsar-manager --redirect.host=http://localhost --redirect.port=9527 insert.stats.interval=600000 --backend.jwt.token=token --jwt.broker.token.mode=PRIVATE --jwt.broker.private.key=file:///path/broker-private.key --jwt.broker.public.key=file:///path/broker-public.key
```

* Method 2. Configure the application.properties file

```
backend.jwt.token=token

jwt.broker.token.mode=PRIVATE
jwt.broker.public.key=file:///path/broker-public.key
jwt.broker.private.key=file:///path/broker-private.key

or 
jwt.broker.token.mode=SECRET
jwt.broker.secret.key=file:///path/broker-secret.key
```

* Method 3: Use Docker only use token.

```
export JWT_TOKEN="your-token"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -v $PWD:/data apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* Method 4: Use Docker with token, public key and private key.

```
export JWT_TOKEN="your-token"
export PRIVATE_KEY="file:///private-key-path"
export PUBLIC_KEY="file:///public-key-path"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e PRIVATE_KEY=$PRIVATE_KEY -e PUBLIC_KEY=$PUBLIC_KEY -v $PWD:/data -v $PWD/private-key-path:/pulsar-manager/private-key-path -v $PWD/public-key-path:/pulsar-manager/public-key-path apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* Method 5: Use Docker with token, secret key.

```
export JWT_TOKEN="your-token"
export SECRET_KEY="file:///secret-key-path"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e PRIVATE_KEY=$PRIVATE_KEY -e PUBLIC_KEY=$PUBLIC_KEY -v $PWD:/data -v $PWD/secret-key-path:/pulsar-manager/secret-key-path apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

[More configurations](https://github.com/apache/pulsar-manager/blob/8b1f26f7d7c725e6d056c41b98235fbc5deb9f49/src/README.md) about backend.
[More configurations](https://github.com/apache/pulsar-manager/blob/master/front-end/README.md) about frontend.

## Login platform

Visit http://localhost:9527