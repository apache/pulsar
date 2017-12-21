---
title: Pulsar on DC/OS
subtitle: Get up and running Pulsar with one command on an Apache Mesos DC/OS cluster
logo: img/dcos/dcos-logo.png
---

[DC/OS](https://dcos.io/) (the <strong>D</strong>ata<strong>C</strong>enter <strong>O</strong>perating <strong>S</strong>ystem) is a distributed operating system used for deploying and managing applications and systems on [Apache Mesos](http://mesos.apache.org/). DC/OS is an open-source tool created and maintained by [Mesosphere](https://mesosphere.com/).

Apache Pulsar is available as a [Marathon Application Groups](https://mesosphere.github.io/marathon/docs/application-groups.html), which runs multiple applications as manageable sets.

## Prerequisites

In order to run Pulsar on DC/OS, we will need:

* DC/OS version [1.9](https://dcos.io/docs/1.9/) or higher
* A [DC/OS cluster](https://dcos.io/install/) with at least three agent nodes
* The [DC/OS CLI tool](https://dcos.io/docs/1.9/usage/cli/install/) installed
* Download and save `PulsarGroups.json` from Pulsar github repo.

Each node in the DC/OS-managed Mesos cluster must have at least:

* 4 CPU
* 4 GB of memory
* 60 GB of total persistent disk 

Or, we could change the configuration in `PulsarGroups.json` according to the current cluster resources.

## Deploy Pulsar use dcos command interface

```shell
$ dcos marathon group add PulsarGroups.json
```

This command will deploy some docker container instances in 3 groups, which consist a Pulsar cluster:

* deploy 3 bookies, 1 `bookie` on each agent node; and 1 `bookie_recovery` instance.
* deploy 3 Pulsar brokers, 1 `broker` on each node; and 1 `broker_admin` instance.
* deploy 1 `prometheus` instance and 1 `grafana` instance.

Since DC/OS is already deployed a zookeeper cluster serving at `master.mesos:2181`, we leverage it, and no need to deploy it ourselves. 
After execute the command above, click on the **Services** tab in the DC/OS [GUI interface](https://docs.mesosphere.com/latest/gui/), whose link is `http://m1.dcos` in this example, and we should see applications deploying.

![DC/OS command executed](/img/dcos/command_execute.png)

![DC/OS command executed2](/img/dcos/command_execute2.png)


## BookKeeper Group

To watch the status of `BookKeeper` Cluster deployment, we could click into the 'bookkeeper' group in parent pulsar group picture.

![DC/OS bookkeeper status](/img/dcos/bookkeeper_status.png)

Now 3 bookies is shown as green colour, which means they are already deployed successfully and running.
 
![DC/OS bookkeeper running](/img/dcos/bookkeeper_run.png)
 
We could also click into each Instance of bookie, to get more detailed info, such as the bookie running log.

![DC/OS bookie log](/img/dcos/bookie_log.png)

And we could also get the BookKeeper cluster information in Zookeeper, whose link is `http://m1.dcos/exhibitor`, and could see 3 bookies under `available` directory.

![DC/OS bookkeeper in zk](/img/dcos/bookkeeper_in_zookeeper.png)


## Pulsar Broker Group

Similar to BookKeeper Group above, click into `brokers` group, and we will see brokers status.

![DC/OS broker status](/img/dcos/broker_status.png)

![DC/OS broker running](/img/dcos/broker_run.png)

We could also click into each Instance of broker, to get more detailed info, such as the broker running log.

![DC/OS broker log](/img/dcos/broker_log.png)

And also get Broker cluster information in Zookeeper, which created directory `loadbalance` and `managed-ledgers`.

![DC/OS broker in zk](/img/dcos/broker_in_zookeeper.png)


## Monitor Group

Monitor Group consist of Prometheus and Grafana. 

![DC/OS monitor status](/img/dcos/monitor_status.png)

### Prometheus

We could click into the instance of `prom` to get the endpoint of Prometheus, which is `192.168.65.121:9090` in this example.

![DC/OS prom endpoint](/img/dcos/prom_endpoint.png)

While click this endpoint above, and we could get into Prometheus dashboard, and the targets `http://192.168.65.121:9090/targets` tab, should contains all bookies and brokers.

![DC/OS prom targets](/img/dcos/prom_targets.png)

### Grafana

We could click into the instance of `grafana` to get the endpoint of Grafana, which is `192.168.65.121:3000` in this example.
 
![DC/OS grafana endpoint](/img/dcos/grafana_endpoint.png)

While click this endpoint above, and we could get into Grafana dashboard.

![DC/OS grafana targets](/img/dcos/grafana_dashboard.png)


## Run simple consumer and producer on DC/OS deployment.

At present we could get a deployed Pulsar cluster. and we can run simple consumer and producer on it now.

### Download and prepare pulsar java tutorial

This code contains a simple Pulsar Consumer and Producer, and the `README.md` contains more information about it.

```shell
$ git clone https://github.com/streamlio/pulsar-java-tutorial.git
```

Change `SERVICE_URL` from "pulsar://localhost:6650" to "pulsar://a1.dcos:6650" in both [ConsumerTutorial.java](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ConsumerTutorial.java) and [ProducerTutorial.java](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java).
"pulsar://a1.dcos:6650" is the endpoint of broker service, we could get the endpoint details from DC/OS GUI of each broker instance. `a1.dcos` is one of DC/OS client agent, which runs a broker, it can also be replaced by the client agent IP address.   

Change the message number from 10 to 10000000 in main method of [ProducerTutorial.java](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java), so it will produce more message.

Now compile the project code using command:

```shell
$ mvn clean package
```

### Run consumer and producer

Execute command to run consumer:

```shell
$ mvn exec:java -Dexec.mainClass="tutorial.ConsumerTutorial"
```

Execute command to run producer:

```shell
$ mvn exec:java -Dexec.mainClass="tutorial.ProducerTutorial"
```

And we could see the producer producing messages and consumer consuming messages.

![DC/OS pulsar producer](/img/dcos/producer.png)

![DC/OS pulsar consumer](/img/dcos/consumer.png)


### View Grafana metric output

While producer and consumer is running, we could get some running metrics information from Grafana

![DC/OS pulsar dashboard](/img/dcos/metrics.png)


## Uninstall Pulsar

We can shut down and uninstall the `pulsar` from DC/OS at any time by 2 ways.

Using DC/OS GUI, we could choose `Delete` at the right end of Pulsar group.

![DC/OS pulsar uninstall](/img/dcos/uninstall.png)


Or we could use command to do the uninstall:

```shell
$ dcos marathon group remove /pulsar
```
