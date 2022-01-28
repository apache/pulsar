---
id: version-2.7.1-administration-pulsar-manager
title: Pulsar Manager
sidebar_label: Pulsar Manager
original_id: administration-pulsar-manager
---

Pulsar Manager is a web-based GUI management and monitoring tool that helps administrators and users manage and monitor tenants, namespaces, topics, subscriptions, brokers, clusters, and so on, and supports dynamic configuration of multiple environments.

> **Note**   
> If you monitor your current stats with [Pulsar dashboard](administration-dashboard.md), you can try to use Pulsar Manager instead. Pulsar dashboard is deprecated.

## Install

The easiest way to use the Pulsar Manager is to run it inside a [Docker](https://www.docker.com/products/docker) container.


```shell
docker pull apachepulsar/pulsar-manager:v0.2.0
docker run -it \
    -p 9527:9527 -p 7750:7750 \
    -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
    apachepulsar/pulsar-manager:v0.2.0
```

* `SPRING_CONFIGURATION_FILE`: Default configuration file for spring.

### Set administrator account and password

 ```shell
CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
curl \
    -H 'X-XSRF-TOKEN: $CSRF_TOKEN' \
    -H 'Cookie: XSRF-TOKEN=$CSRF_TOKEN;' \
    -H "Content-Type: application/json" \
    -X PUT http://localhost:7750/pulsar-manager/users/superuser \
    -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'
```

You can find the docker image in the [Docker Hub](https://github.com/apache/pulsar-manager/tree/master/docker) directory and build an image from the source code as well:

```
git clone https://github.com/apache/pulsar-manager
cd pulsar-manager/front-end
npm install --save
npm run build:prod
cd ..
./gradlew build -x test
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
./gradlew build -x test
```

### Enable JWT authentication

If you want to turn on JWT authentication, configure the following parameters:

* `backend.jwt.token`: token for the superuser. You need to configure this parameter during cluster initialization.
* `jwt.broker.token.mode`: multiple modes of generating token, including PUBLIC, PRIVATE, and SECRET.
* `jwt.broker.public.key`: configure this option if you use the PUBLIC mode.
* `jwt.broker.private.key`: configure this option if you use the PRIVATE mode.
* `jwt.broker.secret.key`: configure this option if you use the SECRET mode.

For more information, see [Token Authentication Admin of Pulsar](http://pulsar.apache.org/docs/en/security-token-admin/).


If you want to enable JWT authentication, use one of the following methods.


* Method 1: use command-line tool

```
wget https://dist.apache.org/repos/dist/release/pulsar/pulsar-manager/pulsar-manager-0.2.0/apache-pulsar-manager-0.2.0-bin.tar.gz
tar -zxvf apache-pulsar-manager-0.2.0-bin.tar.gz
cd pulsar-manager
tar -zxvf pulsar-manager.tar
cd pulsar-manager
cp -r ../dist ui
./bin/pulsar-manager --redirect.host=http://localhost --redirect.port=9527 insert.stats.interval=600000 --backend.jwt.token=token --jwt.broker.token.mode=PRIVATE --jwt.broker.private.key=file:///path/broker-private.key --jwt.broker.public.key=file:///path/broker-public.key 
```
Firstly, [set the administrator account and password](#set-administrator-account-and-password)

Secondly, log in to Pulsar manager through http://localhost:7750/ui/index.html.

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

* Method 3: use Docker and enable token authentication.

```
export JWT_TOKEN="your-token"
docker run -it -p 9527:9527 -p 7750:7750 -e REDIRECT_HOST=http://localhost -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -v $PWD:/data apachepulsar/pulsar-manager:v0.2.0 /bin/sh
```

* `JWT_TOKEN`: the token of superuser configured for the broker. It is generated by the  `bin/pulsar tokens create --secret-key` or `bin/pulsar tokens create --private-key` command.
* `REDIRECT_HOST`: the IP address of the front-end server.
* `REDIRECT_PORT`: the port of the front-end server.
* `DRIVER_CLASS_NAME`: the driver class name of the PostgreSQL database.
* `URL`: the JDBC URL of your PostgreSQL database, such as jdbc:postgresql://127.0.0.1:5432/pulsar_manager. The docker image automatically start a local instance of the PostgreSQL database.
* `USERNAME`: the username of PostgreSQL.
* `PASSWORD`: the password of PostgreSQL.
* `LOG_LEVEL`: the level of log.

* Method 4: use Docker and turn on **token authentication** and **token management** by private key and public key.

```
export JWT_TOKEN="your-token"
export PRIVATE_KEY="file:///pulsar-manager/secret/my-private.key"
export PUBLIC_KEY="file:///pulsar-manager/secret/my-public.key"
docker run -it -p 9527:9527 -p 7750:7750 -e REDIRECT_HOST=http://localhost -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e PRIVATE_KEY=$PRIVATE_KEY -e PUBLIC_KEY=$PUBLIC_KEY -v $PWD:/data -v $PWD/secret:/pulsar-manager/secret apachepulsar/pulsar-manager:v0.2.0 /bin/sh
```

* `JWT_TOKEN`: the token of superuser configured for the broker. It is generated by the `bin/pulsar tokens create --private-key` command.
* `PRIVATE_KEY`: private key path mounted in container, generated by `bin/pulsar tokens create-key-pair` command.
* `PUBLIC_KEY`: public key path mounted in container, generated by `bin/pulsar tokens create-key-pair` command.
* `$PWD/secret`: the folder where the private key and public key generated by the `bin/pulsar tokens create-key-pair` command are placed locally
* `REDIRECT_HOST`: the IP address of the front-end server.
* `REDIRECT_PORT`: the port of the front-end server.
* `DRIVER_CLASS_NAME`: the driver class name of the PostgreSQL database.
* `URL`: the JDBC URL of your PostgreSQL database, such as jdbc:postgresql://127.0.0.1:5432/pulsar_manager. The docker image automatically start a local instance of the PostgreSQL database.
* `USERNAME`: the username of PostgreSQL.
* `PASSWORD`: the password of PostgreSQL.
* `LOG_LEVEL`: the level of log.

* Method 5: use Docker and turn on **token authentication** and **token management** by secret key.


```
export JWT_TOKEN="your-token"
export SECRET_KEY="file:///pulsar-manager/secret/my-secret.key"
docker run -it -p 9527:9527 -p 7750:7750 -e REDIRECT_HOST=http://localhost -e REDIRECT_PORT=9527 -e DRIVER_CLASS_NAME=org.postgresql.Driver -e URL='jdbc:postgresql://127.0.0.1:5432/pulsar_manager' -e USERNAME=pulsar -e PASSWORD=pulsar -e LOG_LEVEL=DEBUG -e JWT_TOKEN=$JWT_TOKEN -e SECRET_KEY=$SECRET_KEY -v $PWD:/data -v $PWD/secret:/pulsar-manager/secret apachepulsar/pulsar-manager:v0.2.0 /bin/sh
```

* `JWT_TOKEN`: the token of superuser configured for the broker. It is generated by the `bin/pulsar tokens create --secret-key` command.
* `SECRET_KEY`: secret key path mounted in container, generated by `bin/pulsar tokens create-secret-key` command.
* `$PWD/secret`: the folder where the secret key generated by the `bin/pulsar tokens create-secret-key` command are placed locally
* `REDIRECT_HOST`: the IP address of the front-end server.
* `REDIRECT_PORT`: the port of the front-end server.
* `DRIVER_CLASS_NAME`: the driver class name of the PostgreSQL database.
* `URL`: the JDBC URL of your PostgreSQL database, such as jdbc:postgresql://127.0.0.1:5432/pulsar_manager. The docker image automatically start a local instance of the PostgreSQL database.
* `USERNAME`: the username of PostgreSQL.
* `PASSWORD`: the password of PostgreSQL.
* `LOG_LEVEL`: the level of log.

* For more information about backend configurations, see [here](https://github.com/apache/pulsar-manager/blob/master/src/README.md).
* For more information about frontend configurations, see [here](https://github.com/apache/pulsar-manager/tree/master/front-end).

## Log in

[Set the administrator account and password](#set-administrator-account-and-password).

Visit http://localhost:9527 to log in.
