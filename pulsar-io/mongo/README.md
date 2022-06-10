## Apache Pulsar MongoDB Connector

### Usage

> reference: https://blog.tericcabrel.com/mongodb-replica-set-docker-compose/

We can use [Docker Compose](https://github.com/docker/compose) to deploy a MongoDB Replica Set.

First, create a directory with the name you want, for example, `mongo`.

```
$ tree mongo -L 2
mongo
├── docker-compose.yml
├── init.sh
└── mongo
    ├── data1
    ├── data2
    └── data3
```

In your folder, create a file called `docker-compose.yml` with the following code:

```yaml
version: '3.8'

services:
  mongo1:
    container_name: mongo1
    image: mongo:4.4
    restart: always
    ports:
      - "27017:27017"
    volumes:
      - ./mongo/data1:/data/db
      - ./init.sh:/scripts/init.sh
    networks:
      - mongo-network
    links:
      - mongo2
      - mongo3
    entrypoint: [ "/usr/bin/mongod", "--bind_ip_all", "--replSet", "dbrs" ]

  mongo2:
    container_name: mongo2
    image: mongo:4.4
    restart: always
    ports:
      - "27018:27017"
    volumes:
      - ./mongo/data2:/data/db
    networks:
      - mongo-network
    entrypoint: [ "/usr/bin/mongod", "--bind_ip_all", "--replSet", "dbrs" ]

  mongo3:
    container_name: mongo3
    image: mongo:4.4
    restart: always
    ports:
      - "27019:27017"
    volumes:
      - ./mongo/data3:/data/db
    networks:
      - mongo-network
    entrypoint: [ "/usr/bin/mongod", "--bind_ip_all", "--replSet", "dbrs" ]

networks:
  mongo-network:
    driver: bridge
```

We created three Docker containers from MongoDB images.
On the first container, in addition to the volume folder for database data,
we map the file in the host called `init.sh` to the container at the location `/scripts/init.sh`.
This file will contain the following command:

- Connect to the mongo instance
- Configure the replica set configure
- Exit

So let's create this file and add the code below:

```shell
#!/bin/bash

mongo <<EOF
var config = {
    "_id": "dbrs",
    "version": 1,
    "members": [
        {
            "_id": 1,
            "host": "mongo1:27017",
            "priority": 3
        },
        {
            "_id": 2,
            "host": "mongo2:27017",
            "priority": 2
        },
        {
            "_id": 3,
            "host": "mongo3:27017",
            "priority": 1
        }
    ]
};
rs.initiate(config, { force: true });
rs.status();
EOF
```

Make this file executable by running the command:

```shell
chmod +x ./init.sh
```

Now, when you want to work, run:

```shell
sudo docker-compose up -d
```

Connect to docker container `mongo1` and execute the file `init.sh`.

```shell
sudo docker exec mongo1 /scripts/init.sh
```

Congratulations, your MongoDB Replica Set is ready to use!
You can connect it by `mongodb://localhost:27017`.
