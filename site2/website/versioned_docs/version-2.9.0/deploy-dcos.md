---
id: version-2.9.0-deploy-dcos
title: Deploy Pulsar on DC/OS
sidebar_label: DC/OS
original_id: deploy-dcos
---

> **Tips**  
> To enable all built-in [Pulsar IO](io-overview.md) connectors in your Pulsar deploymente, we recommend you use `apachepulsar/pulsar-all` image instead of `apachepulsar/pulsar` image; the former has already bundled [all built-in connectors](io-overview.md#working-with-connectors).

[DC/OS](https://dcos.io/) (the <strong>D</strong>ata<strong>C</strong>enter <strong>O</strong>perating <strong>S</strong>ystem) is a distributed operating system for deploying and managing applications and systems on [Apache Mesos](http://mesos.apache.org/). DC/OS is an open-source tool created and maintained by [Mesosphere](https://mesosphere.com/).

Apache Pulsar is available as a [Marathon Application Group](https://mesosphere.github.io/marathon/docs/application-groups.html), which runs multiple applications as manageable sets.

## Prerequisites

You need to prepare your environment before running Pulsar on DC/OS.

* DC/OS version [1.9](https://docs.mesosphere.com/1.9/) or higher
* A [DC/OS cluster](https://docs.mesosphere.com/1.9/installing/) with at least three agent nodes
* The [DC/OS CLI tool](https://docs.mesosphere.com/1.9/cli/install/) installed
* The [`PulsarGroups.json`](https://github.com/apache/pulsar/blob/master/deployment/dcos/PulsarGroups.json) configuration file from the Pulsar GitHub repo.

  ```bash
  $ curl -O https://raw.githubusercontent.com/apache/pulsar/master/deployment/dcos/PulsarGroups.json
  ```

Each node in the DC/OS-managed Mesos cluster must have at least:

* 4 CPU
* 4 GB of memory
* 60 GB of total persistent disk

Alternatively, you can change the configuration in `PulsarGroups.json` accordingly to match your resources of the DC/OS cluster.

## Deploy Pulsar using the DC/OS command interface

You can deploy Pulsar on DC/OS using this command:

```bash
$ dcos marathon group add PulsarGroups.json
```

This command deploys Docker container instances in three groups, which together comprise a Pulsar cluster:

* 3 bookies (1 [bookie](reference-terminology.md#bookie) on each agent node and 1 [bookie recovery](http://bookkeeper.apache.org/docs/latest/admin/autorecovery/) instance)
* 3 Pulsar [brokers](reference-terminology.md#broker) (1 broker on each node and 1 admin instance)
* 1 [Prometheus](http://prometheus.io/) instance and 1 [Grafana](https://grafana.com/) instance


> When you run DC/OS, a ZooKeeper cluster will be running at `master.mesos:2181`, thus you do not have to install or start up ZooKeeper separately.

After executing the `dcos` command above, click the **Services** tab in the DC/OS [GUI interface](https://docs.mesosphere.com/latest/gui/), which you can access at [http://m1.dcos](http://m1.dcos) in this example. You should see several applications during the deployment.

![DC/OS command executed](assets/dcos_command_execute.png)

![DC/OS command executed2](assets/dcos_command_execute2.png)

## The BookKeeper group

To monitor the status of the BookKeeper cluster deployment, click the **bookkeeper** group in the parent **pulsar** group.

![DC/OS bookkeeper status](assets/dcos_bookkeeper_status.png)

At this point, the status of the 3 [bookies](reference-terminology.md#bookie) are green, which means that the bookies have been deployed successfully and are running.
 
![DC/OS bookkeeper running](assets/dcos_bookkeeper_run.png)
 
You can also click each bookie instance to get more detailed information, such as the bookie running log.

![DC/OS bookie log](assets/dcos_bookie_log.png)

To display information about the BookKeeper in ZooKeeper, you can visit [http://m1.dcos/exhibitor](http://m1.dcos/exhibitor). In this example, 3 bookies are under the `available` directory.

![DC/OS bookkeeper in zk](assets/dcos_bookkeeper_in_zookeeper.png)

## The Pulsar broker group

Similar to the BookKeeper group above, click **brokers** to check the status of the Pulsar brokers.

![DC/OS broker status](assets/dcos_broker_status.png)

![DC/OS broker running](assets/dcos_broker_run.png)

You can also click each broker instance to get more detailed information, such as the broker running log.

![DC/OS broker log](assets/dcos_broker_log.png)

Broker cluster information in ZooKeeper is also available through the web UI. In this example, you can see that the `loadbalance` and `managed-ledgers` directories have been created.

![DC/OS broker in zk](assets/dcos_broker_in_zookeeper.png)

## Monitor group

The **monitory** group consists of Prometheus and Grafana.

![DC/OS monitor status](assets/dcos_monitor_status.png)

### Prometheus

Click the instance of `prom` to get the endpoint of Prometheus, which is `192.168.65.121:9090` in this example.

![DC/OS prom endpoint](assets/dcos_prom_endpoint.png)

If you click that endpoint, you can see the Prometheus dashboard. All the bookies and brokers are listed on [http://192.168.65.121:9090/targets](http://192.168.65.121:9090/targets).

![DC/OS prom targets](assets/dcos_prom_targets.png)

### Grafana

Click `grafana` to get the endpoint for Grafana, which is `192.168.65.121:3000` in this example.
 
![DC/OS grafana endpoint](assets/dcos_grafana_endpoint.png)

If you click that endpoint, you can access the Grafana dashboard.

![DC/OS grafana targets](assets/dcos_grafana_dashboard.png)

## Run a simple Pulsar consumer and producer on DC/OS

Now that you have a fully deployed Pulsar cluster, you can run a simple consumer and producer to show Pulsar on DC/OS in action.

### Download and prepare the Pulsar Java tutorial

You can clone a [Pulsar Java tutorial](https://github.com/streamlio/pulsar-java-tutorial) repo. This repo contains a simple Pulsar consumer and producer (you can find more information in the `README` file in this repo).

```bash
$ git clone https://github.com/streamlio/pulsar-java-tutorial
```

Change the `SERVICE_URL` from `pulsar://localhost:6650` to `pulsar://a1.dcos:6650` in both [`ConsumerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ConsumerTutorial.java) file and [`ProducerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java) file.

The `pulsar://a1.dcos:6650` endpoint is for the broker service. You can fetch the endpoint details for each broker instance from the DC/OS GUI. `a1.dcos` is a DC/OS client agent that runs a broker, and you can replace it with the client agent IP address.

Now, you can change the message number from 10 to 10000000 in the main method in [`ProducerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java) file to produce more messages.

Then, you can compile the project code using the command below:

```bash
$ mvn clean package
```

### Run the consumer and producer

Execute this command to run the consumer:

```bash
$ mvn exec:java -Dexec.mainClass="tutorial.ConsumerTutorial"
```

Execute this command to run the producer:

```bash
$ mvn exec:java -Dexec.mainClass="tutorial.ProducerTutorial"
```

You see that the producer is producing messages and the consumer is consuming messages through the DC/OS GUI.

![DC/OS pulsar producer](assets/dcos_producer.png)

![DC/OS pulsar consumer](assets/dcos_consumer.png)

### View Grafana metric output

While the producer and consumer are running, you can access the running metrics from Grafana.

![DC/OS pulsar dashboard](assets/dcos_metrics.png)


## Uninstall Pulsar

You can shut down and uninstall the `pulsar` application from DC/OS at any time in one of the following two ways:

1. Click the three dots at the right end of Pulsar group and choose **Delete** on the DC/OS GUI.

    ![DC/OS pulsar uninstall](assets/dcos_uninstall.png)

2. Use the command below.

    ```bash
    $ dcos marathon group remove /pulsar
    ```
