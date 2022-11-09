---
Id: tutorials-produce-consume
title: Produce and consume messages
sidebar_label: “Tutorials”
—

:::note
This tutorial has the following prerequisites:

[Create tenant](tutorials-tenant.md)
[Create namespace](tutorials-namespace.md)
[Create topic](tutorials-topic.md)
:::


In this tutorial, we will:
Configure the Pulsar client
Create a subscription
Create a producer
Send test messages
Verify the results

In the ${PULSAR_HOME}/conf/client.conf file, replace webServiceUrl and brokerServiceUrl with your service URL.
Create a subscription to consume messages from apache/pulsar/test-topic.
bin/pulsar-client consume -s sub apache/pulsar/test-topic  -n 0
In a new terminal, create a producer and send 10 messages to test-topic.
bin/pulsar-client produce apache/pulsar/test-topic  -m "---------hello apache pulsar-------" -n 10
Verify the results.

----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------
----- got message -----
---------hello apache pulsar-------

Output from the producer side shows the messages have been produced successfully:
18:15:15.489 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 10 messages successfully produced.

Related Topics
[Set up a tenant](tutorials-tenant)
[Create a topic](tutorials-topic)
[Create a namespace](tutorials-namespace)
[Managing Clusters](admin-api-clusters)








