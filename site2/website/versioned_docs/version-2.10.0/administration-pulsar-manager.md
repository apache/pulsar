---
id: administration-pulsar-manager
title: Pulsar Manager
sidebar_label: "Pulsar Manager"
original_id: administration-pulsar-manager
---

Pulsar Manager is a web-based GUI management and monitoring tool that helps administrators and users manage and monitor tenants, namespaces, topics, subscriptions, brokers, clusters, and so on, and supports dynamic configuration of multiple environments.

:::note

If you are monitoring your current stats with [Pulsar dashboard](administration-dashboard), we recommend you use Pulsar Manager instead. Pulsar dashboard is deprecated.

:::

## Install

### Quick Install
The easiest way to use the Pulsar Manager is to run it inside a [Docker](https://www.docker.com/products/docker) container.

```shell

docker pull apachepulsar/pulsar-manager:v0.2.0
docker run -it \
    -p 9527:9527 -p 7750:7750 \
    -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
    apachepulsar/pulsar-manager:v0.2.0

```

* Pulsar Manager is divided into front-end and back-end, the front-end service port is `9527` and the back-end service port is `7750`.
* `SPRING_CONFIGURATION_FILE`: Default configuration file for spring.
* By default, Pulsar Manager uses the `herddb` database. HerdDB is a SQL distributed database implemented in Java and can be found at [herddb.org](https://herddb.org/) for more information.

### Configure Database or JWT authentication
####  Configure Database (optional)

If you have a large amount of data, you can use a custom database, otherwise, some display errors may occur, such as the topic information cannot be displayed when the topic exceeds 10000.
The following is an example of PostgreSQL.

1. Initialize database and table structures using the [file](https://github.com/apache/pulsar-manager/blob/master/src/main/resources/META-INF/sql/postgresql-schema.sql).
2. Download and modify the [configuration file](https://github.com/apache/pulsar-manager/blob/master/src/main/resources/application.properties), then add the PostgreSQL configuration.

```properties

spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url=jdbc:postgresql://127.0.0.1:5432/pulsar_manager
spring.datasource.username=postgres
spring.datasource.password=postgres

```

3. Add a configuration mount and start with a docker image.

```bash

docker pull apachepulsar/pulsar-manager:v0.2.0
docker run -it \
    -p 9527:9527 -p 7750:7750 \
    -v /your-path/application.properties:/pulsar-manager/pulsar-
manager/application.properties
    -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
    apachepulsar/pulsar-manager:v0.2.0

```

####  Enable JWT authentication (optional)

If you want to turn on JWT authentication, configure the `application.properties` file.

```properties

backend.jwt.token=token

jwt.broker.token.mode=PRIVATE
jwt.broker.public.key=file:///path/broker-public.key
jwt.broker.private.key=file:///path/broker-private.key

or 
jwt.broker.token.mode=SECRET
jwt.broker.secret.key=file:///path/broker-secret.key

```

•	`backend.jwt.token`: token for the superuser. You need to configure this parameter during cluster initialization.   
•	`jwt.broker.token.mode`: multiple modes of generating token, including PUBLIC, PRIVATE, and SECRET.  
•	`jwt.broker.public.key`: configure this option if you use the PUBLIC mode.  
•	`jwt.broker.private.key`: configure this option if you use the PRIVATE mode.  
•	`jwt.broker.secret.key`: configure this option if you use the SECRET mode.  
For more information, see [Token Authentication Admin of Pulsar](https://pulsar.apache.org/docs/en/security-token-admin/).

Docker command to add profile and key files mount.

```bash

docker pull apachepulsar/pulsar-manager:v0.2.0
docker run -it \
    -p 9527:9527 -p 7750:7750 \
    -v /your-path/application.properties:/pulsar-manager/pulsar-
manager/application.properties
    -v /your-path/private.key:/pulsar-manager/private.key
    -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
    apachepulsar/pulsar-manager:v0.2.0

```

### Set the administrator account and password

```bash

CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
curl \
   -H 'X-XSRF-TOKEN: $CSRF_TOKEN' \
   -H 'Cookie: XSRF-TOKEN=$CSRF_TOKEN;' \
   -H "Content-Type: application/json" \
   -X PUT http://localhost:7750/pulsar-manager/users/superuser \
   -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'

```

The request parameter in curl command:

```json

{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}

```

- `name` is the Pulsar Manager login username, currently `admin`.
- `password` is the password of the current user of Pulsar Manager, currently `apachepulsar`. The password should be more than or equal to 6 digits.



### Configure the environment
1. Login to the system, Visit http://localhost:9527 to login.  The current default account is  `admin/apachepulsar`

2. Click "New Environment" button to add an environment.

3. Input the "Environment Name". The environment name is used for identifying an environment.

4. Input the "Service URL". The Service URL is the admin service url of your Pulsar cluster.


## Other Installation
### Bare-metal installation

When using binary packages for direct deployment, you can follow these steps.

- Download and unzip the binary package, which is available on the [Pulsar Download](https://pulsar.apache.org/en/download/) page.

  ```bash
  
  	wget https://dist.apache.org/repos/dist/release/pulsar/pulsar-manager/pulsar-manager-0.2.0/apache-pulsar-manager-0.2.0-bin.tar.gz
  	tar -zxvf apache-pulsar-manager-0.2.0-bin.tar.gz
  
  ```

- Extract the back-end service binary package and place the front-end resources in the back-end service directory.

  ```bash
  
  	cd pulsar-manager
  	tar -zxvf pulsar-manager.tar
  	cd pulsar-manager
  	cp -r ../dist ui
  
  ```

- Modify `application.properties` configuration on demand.

  > If you don't want to modify the `application.properties` file, you can add the configuration to the startup parameters via `. /bin/pulsar-manager --backend.jwt.token=token` to add the configuration to the startup parameters. This is a capability of the spring boot framework.

- Start Pulsar Manager

  ```bash
  
  ./bin/pulsar-manager
  
  ```

### Custom docker image installation

You can find the docker image in the [Docker Hub](https://github.com/apache/pulsar-manager/tree/master/docker) directory and build an image from the source code as well:

  ```bash
  
  git clone https://github.com/apache/pulsar-manager
  cd pulsar-manager/front-end
  npm install --save
  npm run build:prod
  cd ..
  ./gradlew build -x test
  cd ..
  docker build -f docker/Dockerfile --build-arg BUILD_DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"` --build-arg VCS_REF=`latest` --build-arg VERSION=`latest` -t apachepulsar/pulsar-manager .
  
  ```

## Configuration



| application.properties              | System env on Docker Image | Desc                                                         | Example                                           |
| ----------------------------------- | -------------------------- | ------------------------------------------------------------ | ------------------------------------------------- |
| backend.jwt.token                   | JWT_TOKEN                  | token for the superuser. You need to configure this parameter during cluster initialization. | `token`                                           |
| jwt.broker.token.mode               | N/A                        | multiple modes of generating token, including PUBLIC, PRIVATE, and SECRET. | `PUBLIC` or `PRIVATE` or `SECRET`.                |
| jwt.broker.public.key               | PUBLIC_KEY                 | configure this option if you use the PUBLIC mode.            | `file:///path/broker-public.key`                  |
| jwt.broker.private.key              | PRIVATE_KEY                | configure this option if you use the PRIVATE mode.           | `file:///path/broker-private.key`                 |
| jwt.broker.secret.key               | SECRET_KEY                 | configure this option if you use the SECRET mode.            | `file:///path/broker-secret.key`                  |
| spring.datasource.driver-class-name | DRIVER_CLASS_NAME          | the driver class name of the database.                       | `org.postgresql.Driver`                           |
| spring.datasource.url               | URL                        | the JDBC URL of your  database.                              | `jdbc:postgresql://127.0.0.1:5432/pulsar_manager` |
| spring.datasource.username          | USERNAME                   | the username of database.                                    | `postgres`                                        |
| spring.datasource.password          | PASSWORD                   | the password of database.                                    | `postgres`                                        |
| N/A                                 | LOG_LEVEL                  | the level of log.                                            | DEBUG                                             |
* For more information about backend configurations, see [here](https://github.com/apache/pulsar-manager/blob/master/src/README).
* For more information about frontend configurations, see [here](https://github.com/apache/pulsar-manager/tree/master/front-end).

