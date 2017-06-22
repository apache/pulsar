Pulsar brokers consist of two components:

1. An HTTP server exposing a [REST interface](../../reference/RestApi) administration and {% popover topic %} lookup.
2. A dispatcher that handles all Pulsar {% popover message %} transfers.

{% popover Brokers %} can be managed via:

* The [`brokers`](../../reference/CliTools#pulsar-admin-brokers) command of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool
* The `/admin/brokers` endpoint of the admin [REST API](../../reference/RestApi)
* The `brokers` method of the {% javadoc PulsarAdmin admin com.yahoo.pulsar.client.admin.PulsarAdmin %} object in the [Java API](../../applications/JavaClient)

In addition to being configurable when you start them up, brokers can also be [dynamically configured](#dynamic-broker-configuration).

{% include admonition.html type="info" content="
See the [Configuration](../../reference/Configuration#broker) page for a full listing of broker-specific configuration parameters.
" %}

### List active brokers

Fetch all available active brokers that are serving traffic.  

#### pulsar-admin


```shell
$ pulsar-admin brokers list use
```

```
broker1.use.org.com:8080
```

###### REST

{% endpoint GET /admin/brokers/:cluster %}

[More info](../../reference/RestApi#/admin/brokers/:cluster)

###### Java

```java
admin.brokers().getActiveBrokers(clusterName)
```


#### list of namespaces owned by a given broker

It finds all namespaces which are owned and served by a given broker.  

###### CLI

```shell
$ pulsar-admin brokers namespaces use \
  --url broker1.use.org.com:8080
```

```json
{
  "my-property/use/my-ns/0x00000000_0xffffffff": {
    "broker_assignment": "shared",
    "is_controlled": false,
    "is_active": true
  }
}
```
###### REST

{% endpoint GET /admin/brokers/:cluster/:broker:/ownedNamespaces %}

###### Java

```java
admin.brokers().getOwnedNamespaces(cluster,brokerUrl);
```

### Dynamic broker configuration

One way to configure a Pulsar {% popover broker %} is to supply a [configuration](../../reference/Configuration#broker) when the broker is [started up](../../reference/CliTools#pulsar-broker).

But since all broker configuration in Pulsar is stored in {% popover ZooKeeper %}, configuration values can also be dynamically updated *while the broker is running*. When you update broker configuration dynamically, ZooKeeper will notify the broker of the change and the broker will then override any existing configuration values.

* The [`brokers`](../../reference/CliTools#pulsar-admin-brokers) command for the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool has a variety of subcommands that enable you to manipulate a broker's configuration dynamically, enabling you to [update config values](#update-dynamic-configuration) and more.
* In the Pulsar admin [REST API](../../reference/RestApi), dynamic configuration is managed through the `/admin/brokers/configuration` endpoint.

### Update dynamic configuration

#### pulsar-admin

The [`update-dynamic-config`](../../reference/CliTools#pulsar-admin-brokers-update-dynamic-config) subcommand will update existing configuration. It takes two arguments: the name of the parameter and the new value. Here's an example for the [`brokerShutdownTimeoutMs`](../../reference/Configuration#broker-brokerShutdownTimeoutMs) parameter:

```shell
$ pulsar-admin brokers update-dynamic-config brokerShutdownTimeoutMs 100
```

#### REST API

{% endpoint POST /admin/brokers/configuration/:configName/:configValue %}

[More info](../../reference/RestApi#/admin/brokers/configuration/:configName/:configValue)

#### Java

```java
admin.brokers().updateDynamicConfiguration(configName, configValue);
```

### List updated values

Fetch a list of all potentially updatable configuraton parameters.

#### pulsar-admin

```shell
$ pulsar-admin brokers list-dynamic-config
brokerShutdownTimeoutMs
```

#### REST API

{% endpoint GET /admin/brokers/configuration %}

[More info](../../reference/RestApi#/admin/brokers/configuration)

#### Java

```java
admin.brokers().getDynamicConfigurationNames();
```

### List all

Fetch a list of all parameters that have been dynamically updated.

#### pulsar-admin

```shell
$ pulsar-admin brokers get-all-dynamic-config
brokerShutdownTimeoutMs:100
```

#### REST API

{% endpoint GET /admin/brokers/configuration/values %}

[More info](../../reference/RestApi#/admin/brokers/configuration/values)

#### Java

```java
admin.brokers().getAllDynamicConfigurations();
```
