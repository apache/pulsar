---
id: version-2.7.3-deploy-docker
title: Deploy a cluster on Docker
sidebar_label: Docker
original_id: deploy-docker
---

To deploy a Pulsar cluster on Docker, complete the following steps:
1. Deploy a ZooKeeper cluster (optional)
2. Initialize cluster metadata
3. Deploy a BookKeeper cluster
4. Deploy one or more Pulsar brokers

## Prepare

To run Pulsar on Docker, you need to create a container for each Pulsar component: ZooKeeper, BookKeeper and broker. You can pull the images of ZooKeeper and BookKeeper separately on [Docker Hub](https://hub.docker.com/), and pull a [Pulsar image](https://hub.docker.com/r/apachepulsar/pulsar-all/tags) for the broker. You can also pull only one [Pulsar image](https://hub.docker.com/r/apachepulsar/pulsar-all/tags) and create three containers with this image. This tutorial takes the second option as an example.

### Pull a Pulsar image
You can pull a Pulsar image from [Docker Hub](https://hub.docker.com/r/apachepulsar/pulsar-all/tags) with the following command.

```
docker pull apachepulsar/pulsar-all:latest
```

### Create three containers
Create containers for ZooKeeper, BookKeeper and broker. In this example, they are named as `zookeeper`, `bookkeeper` and `broker` respectively. You can name them as you want with the `--name` flag. By default, the container names are created randomly.

```
docker run -it --name bookkeeper apachepulsar/pulsar-all:latest /bin/bash
docker run -it --name zookeeper apachepulsar/pulsar-all:latest /bin/bash
docker run -it --name broker apachepulsar/pulsar-all:latest /bin/bash
```

### Create a network
To deploy a Pulsar cluster on Docker, you need to create a `network` and connect the containers of ZooKeeper, BookKeeper and broker to this network. The following command creates the network `pulsar`:

```
docker network create pulsar
```

### Connect containers to network
Connect the containers of ZooKeeper, BookKeeper and broker to the `pulsar` network with the following commands. 

```
docker network connect pulsar zookeeper
docker network connect pulsar bookkeeper
docker network connect pulsar broker
```

To check whether the containers are successfully connected to the network, enter the `docker network inspect pulsar` command.

For detailed information about how to deploy ZooKeeper cluster, BookKeeper cluster, brokers, see [deploy a cluster on bare metal](deploy-bare-metal.md).
