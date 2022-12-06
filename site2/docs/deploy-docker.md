---
id: deploy-docker
title: Deploy a cluster on Docker
sidebar_label: "Docker"
---

You can use two kinds of methods to deploy a Pulsar cluster on Docker.
The first uses Docker commands, while the second uses a `docker-compose.yaml` file.
## Deploy a cluster using Docker commands
To deploy a Pulsar cluster on Docker, you need to complete the following steps:
1. Pull a Pulsar Docker image.
2. Create a network.
3. Create and start the ZooKeeper, bookie, and broker containers.
### Pull a Pulsar image
To run Pulsar on Docker, you need to create a container for each Pulsar component: ZooKeeper, bookie, and the broker. You can pull the images of ZooKeeper and bookie separately on Docker Hub, and pull the Pulsar image for the broker. You can also pull only one Pulsar image and create three containers with this image. This tutorial takes the second option as an example.
You can pull a Pulsar image from Docker Hub with the following command. If you do not want to use some connectors, you can use `apachepulsar/pulsar:latest` there.
```java
docker pull apachepulsar/pulsar-all:latest
```
### Create a network
To deploy a Pulsar cluster on Docker, you need to create a network and connect the containers of ZooKeeper, bookie, and broker to this network.
Use the following command to create the network `pulsar`:
```bash
docker network create pulsar
```
### Create and start containers

#### Create a ZooKeeper container
Create a ZooKeeper container and start the ZooKeeper service.
```bash
docker run -d -p 2181:2181 --net=pulsar -e metadataStoreUrl=zk:zookeeper:2181 -e cluster-name=cluster-a -e managedLedgerDefaultEnsembleSize=1 -e managedLedgerDefaultWriteQuorum=1 -e managedLedgerDefaultAckQuorum=1 -v $(pwd)/data/zookeeper:/pulsar/data/zookeeper --name zookeeper --hostname zookeeper apachepulsar/pulsar-all:latest bash -c "bin/apply-config-from-env.py conf/zookeeper.conf && bin/generate-zookeeper-config.sh conf/zookeeper.conf && exec bin/pulsar zookeeper"
```
#### Initialize the cluster metadata
After creating the ZooKeeper container successfully, you can use the following command to initialize the cluster metadata.
```bash
docker run --net=pulsar --name initialize-pulsar-cluster-metadata apachepulsar/pulsar-all:latest bash -c "bin/pulsar initialize-cluster-metadata \
--cluster cluster-a \
--zookeeper zookeeper:2181 \
--configuration-store zookeeper:2181 \
--web-service-url http://broker:8080 \
--broker-service-url pulsar://broker:6650"
```


#### Create a bookie container
Create a bookie container and start the bookie service.

```bash
docker run -d -e clusterName=cluster-a -e zkServers=zookeeper:2181 --net=pulsar -e metadataServiceUri=metadata-store:zk:zookeeper:2181 -v $(pwd)/data/bookkeeper:/pulsar/data/bookkeeper --name bookie --hostname bookie apachepulsar/pulsar-all:latest    bash -c "bin/apply-config-from-env.py conf/bookkeeper.conf && exec bin/pulsar bookie"
```
#### Create a broker container
Create a broker container and start the broker service.
```bash
docker run -d -p 6650:6650 -p 8080:8080 --net=pulsar  -e metadataStoreUrl=zk:zookeeper:2181  -e zookeeperServers=zookeeper:2181 -e clusterName=cluster-a  -e managedLedgerDefaultEnsembleSize=1 -e managedLedgerDefaultWriteQuorum=1   -e managedLedgerDefaultAckQuorum=1 --name broker --hostname broker apachepulsar/pulsar-all:latest bash -c "bin/apply-config-from-env.py conf/broker.conf && exec bin/pulsar broker"
```

## Deploy a cluster by using `docker-compose.yaml`
Use the following template to create a `docker-compose.yaml` file to deploy a Pulsar cluster quickly. And you can modify or add the configurations in the `environment` section.

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

To create a Pulsar cluster by using the `docker-compose.yaml` file, run the following command.
```bash
docker-compose up -d
```

If you want to destroy the Pulsar cluster with all the containers, run the following command. It will also delete the network that the containers are connected to.
```bash
docker-compose down
```
