---
title: Setting up a local standalone cluster
lead: Run Pulsar as a single JVM process for local development
tags:
- standalone
- local
next: ../ConceptsAndArchitecture
---

For the purposes of local development and testing, you can run a {% popover standalone %} Pulsar {% popover broker %} on your own machine.

{% include admonition.html type="info" title='Pulsar in production?' content="
If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](../../deployment/ClusterSetup) guide." %}

{% include explanations/install-package.md %}

## Starting the cluster

Once you have an up-to-date local copy of the release, you can start up a local cluster using the `pulsar` script, which is stored in the `bin` directory, and specifying that you want to fire up a {% popover standalone %} cluster:

```bash
$ bin/pulsar standalone
```

This will start up a Pulsar {% popover broker %} and the necessary {% popover ZooKeeper %} and {% popover BookKeeper %} components inside of a single Java Virtual Machine (JVM) process.

If the cluster has been successfully started, you should see `INFO`-level log messages like this:

```
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Global Zookeeper cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

{% include admonition.html type="success" title='Automatically created namespace' content='
When you start a local standalone cluster, Pulsar will automatically create a `sample/standalone/ns1` namespace that you can use for development purposes.' %}

## Testing your cluster setup

Pulsar provides a CLI tool called [`pulsar-client`](../../reference/CliTools#pulsar-client) that enables you to do things like send messages to a Pulsar {% popover topic %} in a running cluster. This command will send a simple message saying `Hello, Pulsar` to the `persistent://sample/standalone/ns1/my-topic` topic:

```bash
$ bin/pulsar-client produce \
  persistent://sample/standalone/ns1/my-topic \
  -m 'Hello, Pulsar'
```

If the message has been successfully published to the topic, you should see an {% popover acknowledgement %} like this in the `pulsar-client` logs:

```
2017-06-01 18:18:57,094 - INFO  - [main:CmdProduce@189] - 1 messages successfully produced
```

{% include admonition.html type="success" title="No need to explicitly create new topics"
content="You may have noticed that we did not explicitly create the `my-topic` topic to which we sent the `Hello, Pulsar` message. If you attempt to write a message to a topic that does not yet exist, Pulsar will automatically create that topic for you." %}

## Using Pulsar clients locally

Pulsar currently offers client libraries for [Java](../../applications/JavaClient), [Python](../../applications/PythonClient), and [C++](../../applications/CppClient). If you're running a local {% popover standalone %} cluster, you can use one of these root URLs for interacting with your cluster:

* `http://localhost:8080`
* `pulsar://localhost:6650`

Here's an example producer for a Pulsar {% popover topic %} using the Java client:

```java
String localClusterUrl = "pulsar://localhost:6650";
String namespace = "sample/standalone/ns1"; // This namespace is created automatically
String topic = String.format("persistent://%s/my-topic", namespace);

PulsarClient client = PulsarClient.create(localClusterUrl);
Producer producer = client.createProducer(topic);
```
