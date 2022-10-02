---
id: deploy-docker
title: Deploy a cluster on Docker
sidebar_label: "Docker"
---
## Deploy a cluster on Docker
To deploy a Pulsar cluster on Docker, you need to complete the following steps:
1. Pull the Pulsar Docker image.
2. Create the zookeeper, bookie, broker container by the image
3. Modify the broker.conf and bookkeeper.conf
4. Create ZooKeeper, bookie, and broker containers.
5. Start ZooKeeper and initialize cluster metadata.
6. Start brokers and bookies.

## Prepare
To run Pulsar on Docker, you need to create a container for each Pulsar component: ZooKeeper, BookKeeper and broker. You can pull the images of ZooKeeper and BookKeeper separately on Docker Hub, and pull a Pulsar image for the broker. You can also pull only one Pulsar image and create three containers with this image. This tutorial takes the second option as an example.

## Pull a Pulsar image
You can pull a Pulsar image from Docker Hub with the following command. If you want to use some connectors, you can use apachepulsar/pulsar-all there.

```java
docker pull apachepulsar/pulsar-all:latest
```
## Create containers
Create a ZooKeeper container
```bash
docker run -d --privileged=true -u=root -p 2181:2181 -e metadataStoreUrl=zk:zookeeper:2181 -e cluster-name=cluster-a -e managedLedgerDefaultEnsembleSize=1 -e managedLedgerDefaultWriteQuorum=1 -e managedLedgerDefaultAckQuorum=1 --name zookeeper apachepulsar/pulsar-all:latest /bin/bash
```

* Create broker container

```bash
docker run -d --privileged=true -u=root  -p 6650:6650 -p 8080:8080   -e metadataStoreUrl=zk:zookeeper:2181  -e zookeeperServers=zookeeper:2181 -e clusterName=cluster-a  -e managedLedgerDefaultEnsembleSize=1 -e managedLedgerDefaultWriteQuorum=1   -e managedLedgerDefaultAckQuorum=1 --name broker apachepulsar/pulsar-all:latest /bin/bash
```

* Create a bookie container

```bash
docker run -d --privileged=true -u=root -e clusterName=cluster-a -e zkServers=zookeeper:2181 -e metadataServiceUri=metadata-store:zk:zookeeper:2181  --name bookie apachepulsar/pulsar-all:latest /bin/bash
```
## Create the network
To deploy a Pulsar cluster on Docker, you need to create a network and connect the containers of ZooKeeper, BookKeeper and broker to this network. The following command creates the network pulsar:
```bash
docker network create pulsar
```
Connect the containers of ZooKeeper, BookKeeper and broker to the pulsar network with the following commands.

```bash
docker network connect pulsar zookeeper
```
```bash
docker network connect pulsar bookkeeper
```
```bash
docker network connect pulsar broker
```

## Start ZooKeeper and initialize cluster metadata
Start the ZooKeeper service in the Docker container by running the following commands.
1. Open the ZooKeeper container.
```bash
docker exec -it zookeeper bash
```

2. copy configuration from env
```bash
bin/apply-config-from-env.py conf/zookeeper.conf
```

```bash
bin/generate-zookeeper-config.sh conf/zookeeper.conf
```

3. Start the ZooKeeper service.
```bash
bin/pulsar-daemon start zookeeper
```

4. Initialize the cluster metadata.
```bash
bin/pulsar initialize-cluster-metadata \
         --cluster cluster-a \
         --zookeeper zookeeper:2181 \
         --configuration-store zookeeper:2181 \
         --web-service-url http://broker:8080 \
         --broker-service-url pulsar://broker:6650 \
```
## Start broker and bookie services
Start the broker and bookie services in the Docker container by running the following commands.
### Start broker service
1. Open the broker container
```bash
docker exec -it broker bash
```
2. copy configuration from env
```bash
bin/apply-config-from-env.py conf/broker.conf
```
3. Start the broker service
```bash
bin/pulsar-daemon start broker
```

### Start bookie service
1. Open the bookie container
```bash
docker exec -it bookie bash
```
2. copy configuration from env
```bash
bin/apply-config-from-env.py conf/bookkeeper.conf
```
3. Start the bookie service
```bash
bin/pulsar-daemon start bookie
```

## Deploy a cluster by docker-compose.yaml
The following is a docker-compose.yaml file which can help you to deploy the Pulsar cluster quickly.

* Create a Pulsar cluster by docker-compose.
```bash
docker-compose up -d
```
* Destroy a Pulsar cluster by docker-compose.
```bash
docker-compose down
```

```yaml
version: '3'
networks:
 pulsar:
   driver: bridge
services:
 # Start zookeeper
 zookeeper:
   image: apachepulsar/pulsar:latest
   container_name: zookeeper
   restart: on-failure
   networks:
     - pulsar
   volumes:
     - ./data/zookeeper:/pulsar/data/zookeeper
   environment:
     - metadataStoreUrl=zk:zookeeper:2181
   command: >
     bash -c "bin/apply-config-from-env.py conf/zookeeper.conf && \
              bin/generate-zookeeper-config.sh conf/zookeeper.conf && \
              exec bin/pulsar zookeeper"
   healthcheck:
     test: ["CMD", "bin/pulsar-zookeeper-ruok.sh"]
     interval: 10s
     timeout: 5s
     retries: 30

 # Init cluster metadata
 pulsar-init:
   container_name: pulsar-init
   hostname: pulsar-init
   image: apachepulsar/pulsar:latest
   networks:
     - pulsar
   command: >
     bin/pulsar initialize-cluster-metadata \
                --cluster cluster-a \
                --zookeeper zookeeper:2181 \
                --configuration-store zookeeper:2181 \
                --web-service-url http://broker:8080 \
                --broker-service-url pulsar://broker:6650
   depends_on:
     zookeeper:
       condition: service_healthy

 # Start bookie
 bookie:
   image: apachepulsar/pulsar:latest
   container_name: bookie
   restart: on-failure
   networks:
     - pulsar
   environment:
     - clusterName=cluster-a
     - zkServers=zookeeper:2181
     - metadataServiceUri=metadata-store:zk:zookeeper:2181
   depends_on:
     zookeeper:
       condition: service_healthy
     pulsar-init:
       condition: service_completed_successfully
   # Map the local directory to the container to avoid bookie startup failure due to insufficient container disks.
   volumes:
     - ./data/bookkeeper:/pulsar/data/bookkeeper
   command: bash -c "bin/apply-config-from-env.py conf/bookkeeper.conf
     && exec bin/pulsar bookie"

 # Start broker
 broker:
   image: apachepulsar/pulsar:latest
   container_name: broker
   hostname: broker
   restart: on-failure
   networks:
     - pulsar
   environment:
     - metadataStoreUrl=zk:zookeeper:2181
     - zookeeperServers=zookeeper:2181
     - clusterName=cluster-a
     - managedLedgerDefaultEnsembleSize=1
     - managedLedgerDefaultWriteQuorum=1
     - managedLedgerDefaultAckQuorum=1
     - advertisedAddress=broker
     - advertisedListeners=external:pulsar://127.0.0.1:6650
   depends_on:
     zookeeper:
       condition: service_healthy
     bookie:
       condition: service_started
   ports:
     - "6650:6650"
     - "8080:8080"
   command: bash -c "bin/apply-config-from-env.py conf/broker.conf
     &&  exec bin/pulsar broker"
```



