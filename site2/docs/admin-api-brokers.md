---
id: admin-api-brokers
title: Managing Brokers
sidebar_label: "Brokers"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


:::tip

 This page only shows **some frequently used operations**.

 - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more information, see [Pulsar admin doc](/tools/pulsar-admin/).

 - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

 - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

:::

Pulsar brokers consist of two components:

1. An HTTP server exposing a {@inject: rest:REST:/} interface administration and [topic](reference-terminology.md#topic) lookup.
2. A dispatcher that handles all Pulsar [message](reference-terminology.md#message) transfers.

[Brokers](reference-terminology.md#broker) can be managed via:

* The `brokers` command of the [`pulsar-admin`](/tools/pulsar-admin/) tool
* The `/admin/v2/brokers` endpoint of the admin {@inject: rest:REST:/} API
* The `brokers` method of the `PulsarAdmin` object in the [Java API](client-libraries-java.md)

In addition to being configurable when you start them up, brokers can also be [dynamically configured](#dynamic-broker-configuration).

For a full listing of broker-specific configuration parameters, see the [Configuration](reference-configuration.md#broker) page.

## Brokers resources

### List active brokers

Fetch all available active brokers that are serving traffic with cluster name.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin brokers list use
```

Example output:

```
localhost:8080
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/brokers/:cluster|operation/getActiveBrokers?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().getActiveBrokers(clusterName)
```

</TabItem>

</Tabs>
````

### Get the information of the leader broker

Fetch the information of the leader broker, for example, the service url.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin brokers leader-broker
```

Example output:

```json
{
  "serviceUrl" : "http://localhost:8080"
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/brokers/leaderBroker|operation/getLeaderBroker?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().getLeaderBroker()
```

For the detail of the code above, see [here](https://github.com/apache/pulsar/blob/master/pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/BrokersImpl.java#L80)

</TabItem>

</Tabs>
````

#### list of namespaces owned by a given broker

It finds all namespaces which are owned and served by a given broker.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin brokers namespaces use \
    --url localhost:8080
```

Example output:

```
public/default/0x00000000_0x40000000    [broker_assignment=shared is_controlled=false is_active=true]
public/default/0xc0000000_0xffffffff    [broker_assignment=shared is_controlled=false is_active=true]
public/functions/0x40000000_0x80000000    [broker_assignment=shared is_controlled=false is_active=true]
public/functions/0x00000000_0x40000000    [broker_assignment=shared is_controlled=false is_active=true]
pulsar/standalone/localhost:8080/0x00000000_0xffffffff    [broker_assignment=shared is_controlled=false is_active=true]
pulsar/localhost:8080/0x00000000_0xffffffff    [broker_assignment=shared is_controlled=false is_active=true]
public/functions/0x80000000_0xc0000000    [broker_assignment=shared is_controlled=false is_active=true]
public/default/0x80000000_0xc0000000    [broker_assignment=shared is_controlled=false is_active=true]
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/brokers/:cluster/:broker/ownedNamespaces|operation/getOwnedNamespaes?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().getOwnedNamespaces(cluster,brokerUrl);
```

</TabItem>

</Tabs>
````

### Dynamic broker configuration

One way to configure a Pulsar [broker](reference-terminology.md#broker) is to supply a [configuration](reference-configuration.md#broker) when the broker is [started up](reference-cli-tools.md).

But since all broker configuration in Pulsar is stored in ZooKeeper, configuration values can also be dynamically updated *while the broker is running*. When you update broker configuration dynamically, ZooKeeper will notify the broker of the change and the broker will then override any existing configuration values.

* The `brokers` command for the [`pulsar-admin`](/tools/pulsar-admin/) tool has a variety of subcommands that enable you to manipulate a broker's configuration dynamically, enabling you to [update config values](#update-dynamic-configuration) and more.
* In the Pulsar admin {@inject: rest:REST:/} API, dynamic configuration is managed through the `/admin/v2/brokers/configuration` endpoint.

### Update dynamic configuration

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

The [`update-dynamic-config`](/tools/pulsar-admin/) subcommand will update existing configuration. It takes two arguments: the name of the parameter and the new value using the `config` and `value` flag respectively. Here's an example of the [`brokerShutdownTimeoutMs`](reference-configuration.md#broker-brokerShutdownTimeoutMs) parameter:

```shell
pulsar-admin brokers update-dynamic-config --config brokerShutdownTimeoutMs --value 100
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/brokers/configuration/:configName/:configValue|operation/updateDynamicConfiguration?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().updateDynamicConfiguration(configName, configValue);
```

</TabItem>

</Tabs>
````

### List updated values

Fetch a list of all potentially updatable configuration parameters.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin brokers list-dynamic-config
```

Example output:

```
forceDeleteNamespaceAllowed
loadBalancerMemoryResourceWeight
allowAutoTopicCreation
brokerDeleteInactivePartitionedTopicMetadataEnabled
managedLedgerInactiveLedgerRolloverTimeSeconds
loadBalancerNamespaceBundleMaxMsgRate
resourceUsageTransportPublishIntervalInSecs
# omit...
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/brokers/configuration|operation/getDynamicConfigurationName?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().getDynamicConfigurationNames();
```

</TabItem>

</Tabs>
````

### List all

Fetch a list of all parameters that have been dynamically updated.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin brokers get-all-dynamic-config
```
Example output:

```
brokerShutdownTimeoutMs    100
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/brokers/configuration/values|operation/getAllDynamicConfigurations?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.brokers().getAllDynamicConfigurations();
```

</TabItem>

</Tabs>
````
