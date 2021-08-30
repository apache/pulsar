---
id: version-2.7.2-deploy-dcos
title: Deploy Pulsar on DC/OS
sidebar_label: DC/OS
original_id: deploy-dcos
---

> ### Tips
>
> If you want to enable all builtin [Pulsar IO](io-overview.md) connectors in your Pulsar deployment, you can choose to use `apachepulsar/pulsar-all` image instead of
> `apachepulsar/pulsar` image. `apachepulsar/pulsar-all` image has already bundled [all builtin connectors](io-overview.md#working-with-connectors).

[DC/OS](https://dcos.io/) (the <strong>D</strong>ata<strong>C</strong>enter <strong>O</strong>perating <strong>S</strong>ystem) is a distributed operating system used for deploying and managing applications and systems on [Apache Mesos](http://mesos.apache.org/). DC/OS is an open-source tool that [Mesosphere](https://mesosphere.com/) creates and maintains .

Apache Pulsar is available as a [Marathon Application Group](https://mesosphere.github.io/marathon/docs/application-groups.html), which runs multiple applications as manageable sets.

## Prerequisites

In order to run Pulsar on DC/OS, you need the following:

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

Alternatively, you can change the configuration in `PulsarGroups.json` according to match your resources of DC/OS cluster.

## Deploy Pulsar using the DC/OS command interface

You can deploy Pulsar on DC/OS using this command:

```bash
$ dcos marathon group add PulsarGroups.json
```

This command deploys Docker container instances in three groups, which together comprise a Pulsar cluster:

* 3 bookies (1 [bookie](reference-terminology.md#bookie) on each agent node and 1 [bookie recovery](http://bookkeeper.apache.org/docs/latest/admin/autorecovery/) instance)
* 3 Pulsar [brokers](reference-terminology.md#broker) (1 broker on each node and 1 admin instance)
* 1 [Prometheus](http://prometheus.io/) instance and 1 [Grafana](https://grafana.com/) instance


> When you run DC/OS, a ZooKeeper cluster already runs at `master.mesos:2181`, thus you do not have to install or start up ZooKeeper separately.

After executing the `dcos` command above, click on the **Services** tab in the DC/OS [GUI interface](https://docs.mesosphere.com/latest/gui/), which you can access at [http://m1.dcos](http://m1.dcos) in this example. You should see several applications in the process of deploying.

![DC/OS command executed](assets/dcos_command_execute.png)

![DC/OS command executed2](assets/dcos_command_execute2.png)

## The BookKeeper group

To monitor the status of the BookKeeper cluster deployment, click on the **bookkeeper** group in the parent **pulsar** group.

![DC/OS bookkeeper status](assets/dcos_bookkeeper_status.png)

At this point, 3 [bookies](reference-terminology.md#bookie) should be shown as green, which means that the bookies have been deployed successfully and are now running.
 
![DC/OS bookkeeper running](assets/dcos_bookkeeper_run.png)
 
You can also click into each bookie instance to get more detailed information, such as the bookie running log.

![DC/OS bookie log](assets/dcos_bookie_log.png)

To display information about the BookKeeper in ZooKeeper, you can visit [http://m1.dcos/exhibitor](http://m1.dcos/exhibitor). In this example, 3 bookies are under the `available` directory.

![DC/OS bookkeeper in zk](assets/dcos_bookkeeper_in_zookeeper.png)

## The Pulsar broker Group

Similar to the BookKeeper group above, click into the **brokers** to check the status of the Pulsar brokers.

![DC/OS broker status](assets/dcos_broker_status.png)

![DC/OS broker running](assets/dcos_broker_run.png)

You can also click into each broker instance to get more detailed information, such as the broker running log.

![DC/OS broker log](assets/dcos_broker_log.png)

Broker cluster information in Zookeeper is also available through the web UI. In this example, you can see that the `loadbalance` and `managed-ledgers` directories have been created.

![DC/OS broker in zk](assets/dcos_broker_in_zookeeper.png)

## Monitor Group

The **monitory** group consists of Prometheus and Grafana.

![DC/OS monitor status](assets/dcos_monitor_status.png)

### Prometheus

Click into the instance of `prom` to get the endpoint of Prometheus, which is `192.168.65.121:9090` in this example.

![DC/OS prom endpoint](assets/dcos_prom_endpoint.png)

If you click that endpoint, you can see the Prometheus dashboard. The [http://192.168.65.121:9090/targets](http://192.168.65.121:9090/targets) URL display all the bookies and brokers.

![DC/OS prom targets](assets/dcos_prom_targets.png)

### Grafana

Click into `grafana` to get the endpoint for Grafana, which is `192.168.65.121:3000` in this example.
 
![DC/OS grafana endpoint](assets/dcos_grafana_endpoint.png)

If you click that endpoint, you can access the Grafana dashboard.

![DC/OS grafana targets](assets/dcos_grafana_dashboard.png)

## Run a simple Pulsar consumer and producer on DC/OS

Now that you have a fully deployed Pulsar cluster, you can run a simple consumer and producer to show Pulsar on DC/OS in action.

### Download and prepare the Pulsar Java tutorial

You can clone a [Pulsar Java tutorial](https://github.com/streamlio/pulsar-java-tutorial) repo. This repo contains a simple Pulsar consumer and producer (you can find more information in the `README` file of the repo).

```bash
$ git clone https://github.com/streamlio/pulsar-java-tutorial
```

Change the `SERVICE_URL` from `pulsar://localhost:6650` to `pulsar://a1.dcos:6650` in both [`ConsumerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ConsumerTutorial.java) and [`ProducerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java).
The `pulsar://a1.dcos:6650` endpoint is for the broker service. You can fetch the endpoint details for each broker instance from the DC/OS GUI. `a1.dcos` is a DC/OS client agent, which runs a broker. The client agent IP address can also replace this.

Now, change the message number from 10 to 10000000 in main method of [`ProducerTutorial.java`](https://github.com/streamlio/pulsar-java-tutorial/blob/master/src/main/java/tutorial/ProducerTutorial.java) so that it can produce more messages.

Now compile the project code using the command below:

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

You can see the producer producing messages and the consumer consuming messages through the DC/OS GUI.

![DC/OS pulsar producer](assets/dcos_producer.png)

![DC/OS pulsar consumer](assets/dcos_consumer.png)

### View Grafana metric output

While the producer and consumer run, you can access running metrics information from Grafana.

![DC/OS pulsar dashboard](assets/dcos_metrics.png)


## Uninstall Pulsar

You can shut down and uninstall the `pulsar` application from DC/OS at any time in the following two ways:

1. Using the DC/OS GUI, you can choose **Delete** at the right end of Pulsar group.

    ![DC/OS pulsar uninstall](assets/dcos_uninstall.png)

2. You can use the following command:

    ```bash
    $ dcos marathon group remove /pulsar
    ```
