---
id: administration-pulsar-manager
title: Pulsar Manager
sidebar_label: Pulsar Manager
---

Pulsar Manager is a web-based GUI management and monitoring tool that helps administrators and users manage and monitor tenants, namespaces, topics, subscriptions, brokers, clusters, and so on, and supports dynamic configuration of multiple environments.

> Note   
> If you monitor your current stats with [Pulsar dashboard](administration-dashboard.md), you can try to use Pulsar Manager instead. Pulsar dashboard is deprecated.

## Install

The easiest way to use the Pulsar Manager is to run it inside a [Docker](https://www.docker.com/products/docker) container.


```
docker pull apachepulsar/pulsar-manager:v0.1.0
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.0.104 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -v $PWD:/data apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* REDIRECT_HOST: the IP address of the front-end server.

* REDIRECT_PORT: the port of the front-end server.

* DRIVER_CLASS_NAME: the driver class name of PostgreSQL.

* URL: the URL of PostgreSQL JDBC, For example, `jdbc:postgresql://127.0.0.1:5432/pulsar_manager`.

* USERNAME: the username of PostgreSQL.

* PASSWORD: the password of PostgreSQL.

* LOG_LEVEL: level of log.

You can find the in the [Docker](https://github.com/apache/pulsar-manager/tree/master/docker) directory and build an image from scratch as well:

```
git clone https://github.com/apache/pulsar-manager
cd pulsar-manager
./gradlew build -x test
cd front-end
npm install --save
npm run build:prod
cd ..
docker build -f docker/Dockerfile --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` --build-arg VCS_REF=`latest` --build-arg VERSION=`latest` -t apachepulsar/pulsar-manager .
```

### Use custom databases

If you have a large amount of data, you can use a custom database. The following is an example of PostgreSQL.   

1. Initialize database and table structures using the [file](https://github.com/apache/pulsar-manager/tree/master/src/main/resources/META-INF/sql/postgresql-schema.sql).

2. Modify the [configuration file](https://github.com/apache/pulsar-manager/blob/master/src/main/resources/application.properties) and add PostgreSQL configuration.

```
spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://127.0.0.1:5432/pulsar_manager
spring.datasource.username=postgres
spring.datasource.password=postgres
```

3. Compile to generate a new executable jar package.

```
./gradlew -x build -x test
```

### Enable JWT authentication

If you want to turn on JWT authentication, configure the following parameters:

* `backend.jwt.token`:  token for the superuser. You need to configure this parameter during cluster initialization.
* `jwt.broker.token.mode`:  two modes of generating token, SECRET and PRIVATE.
* `jwt.broker.public.key`: configure this option if you are using the PRIVATE mode.
* `jwt.broker.private.key`: configure this option if you are using the PRIVATE mode.
* `jwt.broker.secret.key`: configure this option if you are using the SECRET mode.

For more information, see [Token Authentication Admin of Pulsar](http://pulsar.apache.org/docs/en/security-token-admin/).


If you want to enable JWT authentication, use one of the following methods.


* Method 1: use command-line tool

```
./build/distributions/pulsar-manager/bin/pulsar-manager --redirect.host=http://localhost --redirect.port=9527 insert.stats.interval=600000 --backend.jwt.token=token --jwt.broker.token.mode=PRIVATE --jwt.broker.private.key=file:///path/broker-private.key --jwt.broker.public.key=file:///path/broker-public.key
```

* Method 2: configure the application.properties file

```
backend.jwt.token=token

jwt.broker.token.mode=PRIVATE
jwt.broker.public.key=file:///path/broker-public.key
jwt.broker.private.key=file:///path/broker-private.key

or 
jwt.broker.token.mode=SECRET
jwt.broker.secret.key=file:///path/broker-secret.key
```

* Method 3: use Docker and turn on token authentication.

```
export JWT_TOKEN="your-token"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -v $PWD:/data apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* Method 4: use Docker and turn on **token authentication** and **token management** by private key and public key.

```
export JWT_TOKEN="your-token"
export PRIVATE_KEY="file:///private-key-path"
export PUBLIC_KEY="file:///public-key-path"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e PRIVATE_KEY=$PRIVATE_KEY -e PUBLIC_KEY=$PUBLIC_KEY -v $PWD:/data -v $PWD/private-key-path:/pulsar-manager/private-key-path -v $PWD/public-key-path:/pulsar-manager/public-key-path apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* Method 5: use Docker and turn on **token authentication** and **token management** by secret key.

```
export JWT_TOKEN="your-token"
export SECRET_KEY="file:///secret-key-path"
docker run -it -p 9527:9527 -e REDIRECT_HOST=http://192.168.55.182 -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e PRIVATE_KEY=$PRIVATE_KEY -e PUBLIC_KEY=$PUBLIC_KEY -v $PWD:/data -v $PWD/secret-key-path:/pulsar-manager/secret-key-path apachepulsar/pulsar-manager:v0.1.0 /bin/sh
```

* For more information about backend configurations, see [here](https://github.com/apache/pulsar-manager/blob/8b1f26f7d7c725e6d056c41b98235fbc5deb9f49/src/README.md).
* For more information about frontend configurations, see [here](https://github.com/apache/pulsar-manager/blob/master/front-end/README.md).

## Log in

Visit http://localhost:9527 to log in.
