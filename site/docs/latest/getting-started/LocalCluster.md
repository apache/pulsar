---
title: Setting up a local standalone cluster
lead: Run Pulsar as a single JVM process for local development
tags:
- standalone
- local
next: ../ConceptsAndArchitecture
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

For the purposes of local development and testing, you can run Pulsar in {% popover standalone %} mode on your own machine. Standalone mode includes a Pulsar {% popover broker %} as well as the necessary {% popover ZooKeeper %} and {% popover BookKeeper %} components running inside of a single Java Virtual Machine (JVM) process.

{% include admonition.html type="info" title='Pulsar in production?' content="
If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](../../deployment/InstanceSetup) guide." %}

{% include explanations/install-package.md %}

## Starting the cluster

Once you have an up-to-date local copy of the release, you can start up a local cluster using the [`pulsar`](../../reference/CliTools#pulsar) command, which is stored in the `bin` directory, and specifying that you want to start up Pulsar in {% popover standalone %} mode:

```bash
$ bin/pulsar standalone
```

If Pulsar has been successfully started, you should see `INFO`-level log messages like this:

```
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

{% include admonition.html type="success" title='Automatically created namespace' content='
When you start a local standalone cluster, Pulsar will automatically create a `public/default` [namespace](../ConceptsAndArchitecture#namespace) that you can use for development purposes. All Pulsar topics are managed within namespaces. For more info, see [Topics](../ConceptsAndArchitecture#topics).' %}

## Testing your cluster setup

Pulsar provides a CLI tool called [`pulsar-client`](../../reference/CliTools#pulsar-client) that enables you to do things like send messages to a Pulsar {% popover topic %} in a running cluster. This command will send a simple message saying `hello-pulsar` to the `my-topic` topic:

```bash
$ bin/pulsar-client produce my-topic \
  --messages "hello-pulsar"
```

If the message has been successfully published to the topic, you should see a confirmation like this in the `pulsar-client` logs:

```
13:09:39.356 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 1 messages successfully produced
```

{% include admonition.html type="success" title="No need to explicitly create new topics"
content="You may have noticed that we did not explicitly create the `my-topic` topic to which we sent the `hello-pulsar` message. If you attempt to write a message to a topic that does not yet exist, Pulsar will automatically create that topic for you." %}

## Using Pulsar clients locally

Pulsar currently offers client libraries for [Java](../../clients/Java), [Python](../../clients/Python), and [C++](../../clients/Cpp). If you're running a local {% popover standalone %} cluster, you can use one of these root URLs for interacting with your cluster:

* `http://localhost:8080`
* `pulsar://localhost:6650`

Here's an example producer for a Pulsar {% popover topic %} using the [Java](../../clients/Java) client:

```java
String localClusterUrl = "pulsar://localhost:6650";

PulsarClient client = PulsarClient.builder().serviceUrl(localClusterUrl).build();
Producer<byte[]> producer = client.newProducer().topic("my-topic").create();
```

Here's an example [Python](../../clients/Python) producer:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('my-topic')
```

Finally, here's an example [C++](../../clients/Cpp) producer:

```cpp
Client client("pulsar://localhost:6650");
Producer producer;
Result result = client.createProducer("my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}
```
