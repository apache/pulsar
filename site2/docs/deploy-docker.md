---
id: deploy-docker
title: Deploy a cluster on Docker
sidebar_label: "Docker"
---
## Deploy a cluster on Docker
To deploy a Pulsar cluster on Docker, you need to complete the next steps:
1. pull the pulsar docker image
2. create the zookeeper, bookie, broker container by the image
3. create a network, and make the container connects to it.
4. start the zookeeper, and then init the cluster metadata
5. start the broker and bookie

## Prepare
To run Pulsar on Docker, you need to create a container for each Pulsar component: ZooKeeper, BookKeeper and broker. You can pull the images of ZooKeeper and BookKeeper separately on Docker Hub, and pull a Pulsar image for the broker. You can also pull only one Pulsar image and create three containers with this image. This tutorial takes the second option as an example.

## Pull a Pulsar image
You can pull a Pulsar image from Docker Hub with the following command. If you want to use some connectors, you can use apachepulsar/pulsar-all there.

```java
docker pull apachepulsar/pulsar-all:latest
```
## Create containers
* Create zookeeper container

```
docker run -d --privileged=true -u=root -e PULSAR_PREFIX_metadataStoreUrl=zk:zookeeper:2181 -e PULSAR_PREFIX_cluster-name=cluster-a -e PULSAR_PREFIX_managedLedgerDefaultEnsembleSize=1 -e PULSAR_PREFIX_managedLedgerDefaultWriteQuorum=1 -e PULSAR_PREFIX_managedLedgerDefaultAckQuorum=1 --name zookeeper apachepulsar/pulsar-all:latest /bin/bash
```

* Create broker container

```
docker run -d --privileged=true -u=root -e PULSAR_PREFIX_metadataStoreUrl=zk:zookeeper:2181 -e PULSAR_PREFIX_cluster-name=cluster-a -e PULSAR_PREFIX_managedLedgerDefaultEnsembleSize=1 -e PULSAR_PREFIX_managedLedgerDefaultWriteQuorum=1 -e PULSAR_PREFIX_managedLedgerDefaultAckQuorum=1 --name broker apachepulsar/pulsar-all:latest /bin/bash
```

* Create bookie container

```
docker run -d --privileged=true -u=root -e PULSAR_PREFIX_zkServers=zookeeper:2181 -e PULSAR_PREFIX_metadataServiceUri=metadata-store:zk:zookeeper:2181  --name bookie apachepulsar/pulsar-all:latest /bin/bash
```
## Create the network
To deploy a Pulsar cluster on Docker, you need to create a network and connect the containers of ZooKeeper, BookKeeper and broker to this network. The following command creates the network pulsar:
```
docker network create pulsar
```
Connect the containers of ZooKeeper, BookKeeper and broker to the pulsar network with the following commands.

```
docker network connect pulsar zookeeper
```
```
docker network connect pulsar bookkeeper
```
```
docker network connect pulsar broker
```

## start the zookeeper, and init the cluster metadata
Start the zookeeper service in the docker container by the following commands:
1. Open the zookeeper container
```
docker exec -it zookeeper bash
```
2. Start the zookeeper service
```
bin/pulsar-daemon start zookeeper
```
3. init the cluster metadata
```
 bin/pulsar initialize-cluster-metadata \
          --cluster cluster-a \
          --zookeeper zookeeper:2181 \
          --configuration-store zookeeper:2181 \
          --web-service-url http://broker:8080 \
          --broker-service-url pulsar://broker:6650 \
```
## start the broker and bookie service
Start the broker and bookie service in the docker container by the following commands:
### Start broker service
1. Open the broker container
```
docker exec -it broker bash
```
2. Start the broker service
```
bin/pulsar-daemon start broker
```

### Start bookie service
1. Open the bookie container
```
docker exec -it bookie bash
```
2. Start the bookie service
```
bin/pulsar-daemon start bookie
```


