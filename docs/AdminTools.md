
# Pulsar admin tool

<!-- TOC depthFrom:1 depthTo:5 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Pulsar admin tool](#pulsar-admin-tool)
	- [What is Pulsar admin tool?](#what-is-pulsar-admin-tool)
		- [How does it work?](#how-does-it-work)
	- [Prerequisite](#prerequisite)
		- [Pulsar admin CLI tool](#pulsar-admin-cli-tool)
		- [REST API](#rest-api)
		- [Java API](#java-api)
		- [Pulsar entities](#pulsar-entities)
		- [Brokers](#brokers)
			- [list of active brokers](#list-of-active-brokers)
			- [list of namespaces owned by a given broker](#list-of-namespaces-owned-by-a-given-broker)
			- [update dynamic configuration](#update-dynamic-configuration)
			- [get list of dynamic configuration name](#get-list-of-dynamic-configuration-name)
			- [get value of dynamic configurations](#get-value-of-dynamic-configurations)
		- [Properties](#properties)
			- [list existing properties](#list-existing-properties)
			- [create property](#create-property)
			- [get property](#get-property)
			- [update property](#update-property)
			- [delete property](#delete-property)
		- [Clusters](#clusters)
			- [provision cluster](#provision-cluster)
			- [get cluster](#get-cluster)
			- [update cluster](#update-cluster)
			- [delete cluster](#delete-cluster)
			- [list all clusters](#list-all-clusters)
		- [Namespace](#namespace)
			- [create namespace](#create-namespace)
			- [get namespace](#get-namespace)
			- [list all namespaces under property](#list-all-namespaces-under-property)
			- [list all namespaces under cluster](#list-all-namespaces-under-cluster)
			- [delete namespace](#delete-namespace)
			- [grant permission](#grant-permission)
			- [get permission](#get-permission)
			- [revoke permission](#revoke-permission)
			- [set replication cluster](#set-replication-cluster)
			- [get replication cluster](#get-replication-cluster)
			- [set backlog quota policies](#set-backlog-quota-policies)
			- [get backlog quota policies](#get-backlog-quota-policies)
			- [remove backlog quota policies](#remove-backlog-quota-policies)
			- [set persistence policies](#set-persistence-policies)
			- [get persistence policies](#get-persistence-policies)
			- [unload namespace bundle](#unload-namespace-bundle)
			- [set message-ttl](#set-message-ttl)
			- [get message-ttl](#get-message-ttl)
			- [split bundle](#split-bundle)
			- [clear backlog](#clear-backlog)
			- [clear bundle backlog](#clear-bundle-backlog)
			- [set retention](#set-retention)
			- [get retention](#get-retention)
		- [Persistent](#persistent)
			- [create partitioned topic](#create-partitioned-topic)
			- [get partitioned topic](#get-partitioned-topic)
			- [delete partitioned topic](#delete-partitioned-topic)
			- [Delete topic](#delete-topic)
			- [List of topics](#list-of-topics)
			- [Grant permission](#grant-permission)
			- [Get permission](#get-permission)
			- [Revoke permission](#revoke-permission)
			- [get partitioned topic stats](#get-partitioned-topic-stats)
			- [Get stats](#get-stats)
			- [Get detailed stats](#get-detailed-stats)
			- [Peek messages](#peek-messages)
			- [Skip messages](#skip-messages)
			- [Skip all messages](#skip-all-messages)
			- [Expire messages](#expire-messages)
			- [Expire all messages](#expire-all-messages)
			- [Reset cursor](#reset-cursor)
			- [Lookup of topic](#lookup-of-topic)
			- [Get subscriptions](#get-subscriptions)
			- [unsubscribe](#unsubscribe)
		- [Namespace isolation policy](#namespace-isolation-policy)
			- [create/update ns-isolation policy](#createupdate-ns-isolation-policy)
			- [get ns-isolation policy](#get-ns-isolation-policy)
			- [delete ns-isolation policy](#delete-ns-isolation-policy)
			- [list all ns-isolation policy](#list-all-ns-isolation-policy)
		- [Resource-Quotas](#resource-quotas)
			- [set namespace resource quota](#set-namespace-resource-quota)
			- [get namespace resource quota](#get-namespace-resource-quota)
			- [reset namespace resource quota](#reset-namespace-resource-quota)
		- [Pulsar client tool](#pulsar-client-tool)
			- [produce message command](#produce-message-command)
			- [consume message command](#consume-message-command)

<!-- /TOC -->

## What is Pulsar admin tool?

Pulsar admin tool is a command line utility which provides an interface
between Pulsar-broker and administration to configure and monitor
various entities of broker such as properties, clusters, topics,
namespaces, etc. It is an admin tool which will be useful to onboard
application on Pulsar by allowing creation of property, cluster and
namespace. It also helps to administrate status and usage of topics.
Other than CLI (command line interface), there are other two
alternatives such as REST api and java-api also available to manage and
monitor above described entities. Hence, in this document we will walk
through how to manage Pulsar entities using CLI, REST api and Java-api.

### How does it work?

As we discussed above, Pulsar-broker also exposes admin REST api which
allows similar functionality to configure and administrate various
entities of Pulsar. Pulsar admin CLI tool calls these REST apis using
its java-client api to execute admin commands. You can learn more about
list of available REST apis in *swagger-documentation*.

## Prerequisite

### Pulsar admin CLI tool

As we discussed *Pulsar-Security* in other section which allows
Pulsar-broker to authenticate and authorize incoming requests.
Admin-tool calls Pulsar-broker’s REST api to execute list of commands.
However, if Pulsar-broker has security enabled then admin-tool has to
pass additional information while calling broker’s REST apis in order to
get requests authenticated. Therefore, make sure that you have this
information correctly configured in `conf/client.conf` file.

Now, you are all set to use Pulsar-admin CLI tool. Go to below directory
to start with admin tool.

```shell
$ bin/pulsar-admin --help
```

In earlier section, we also talked about other alternative approaches to
manage Pulsar-entities - REST api and java-api. In this document we will
also talk about REST api end-points and java-api’s snippet to manage the
Pulsar entities.

### REST API

You can learn about Pulsar-admin REST api definition at
*swagger-documentation.* However, in this document we will see use of
each API and how admin CLI command maps to its appropriate REST api.

### Java API

Java-api can be accessed by : ```com.yahoo.pulsar.client.admin.PulsarAdmin```

Below code snippet shows how to initialize *PulsarAdmin* and later in
the document we will see how to use it in order to manage Pulsar
entities.


  ```java
  URL url = new URL("http://localhost:8080");
  String authPluginClassName = "com.org.MyAuthPluginClass"; //Pass auth-plugin class fully-qualified name if Pulsar-security enabled
  String authParams = "param1=value1";//Pass auth-param if auth-plugin class requires it
  boolean useTls = false;
  boolean tlsAllowInsecureConnection = false;
  String tlsTrustCertsFilePath = null;

  ClientConfiguration config = new ClientConfiguration();
  config.setAuthentication(authPluginClassName, authParams);
  config.setUseTls(useTls);
  config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
  config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

  PulsarAdmin admin = new PulsarAdmin(url, config);
  ```


### Pulsar entities


We can mainly access following broker’s entities using admin command
line tool. If you are not already aware about any of following entity
then you can read in details at [Getting-started-guide](GettingStarted.md).

### Brokers

It allows to get list of active brokers and list of namespaces owned by
a given broker.

#### list of active brokers
It fetches available active brokers those are serving traffic.  

###### CLI

```
$ pulsar-admin brokers list use
```

```
broker1.use.org.com:8080
```

###### REST

```
GET /admin/brokers/{cluster}
```

###### Java

```java
admin.brokers().getActiveBrokers(clusterName)
```


#### list of namespaces owned by a given broker

It finds all namespaces which are owned and served by a given broker.  

###### CLI

```
$ pulsar-admin brokers namespaces --url broker1.use.org.com:8080 use
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

```
GET /admin/brokers/{cluster}/{broker}/ownedNamespaces
```

###### Java

```java
admin.brokers().getOwnedNamespaces(cluster,brokerUrl)
```

#### update dynamic configuration
Broker can locally override value of updatable dynamic service-configurations that are stored into zookeeper. This interface allows to change the value of broker's dynamic-configuration into the zookeeper. Broker receives zookeeper-watch with new changed value and broker updates new value locally.

###### CLI

```
$ pulsar-admin brokers update-dynamic-config brokerShutdownTimeoutMs 100
```

```
N/A
```

###### REST

```
GET /admin/brokers/configuration/{configName}/{configValue}
```

###### Java

```java
admin.brokers().updateDynamicConfiguration(configName, configValue)
```

#### get list of dynamic configuration name
It gives list of updatable dynamic service-configuration name.

###### CLI

```
$ pulsar-admin brokers list-dynamic-config
```

```
brokerShutdownTimeoutMs
```

###### REST

```
GET /admin/brokers/configuration
```

###### Java

```java
admin.brokers().getDynamicConfigurationNames()
```

#### get value of dynamic configurations
It gives value of all dynamic configurations stored in zookeeper

###### CLI

```
$ pulsar-admin brokers get-all-dynamic-config
```

```
brokerShutdownTimeoutMs:100
```

###### REST

```
GET /admin/brokers/configuration/values
```

###### Java

```java
admin.brokers().getAllDynamicConfigurations()
```

#### Update dynamic configuration
Broker can locally override value of updatable dynamic service-configurations that are stored into zookeeper. This interface allows to change the value of broker's dynamic-configuration into the zookeeper. Broker receives zookeeper-watch with new changed value and broker updates new value locally.

###### CLI

```
$ pulsar-admin brokers update-dynamic-config brokerShutdownTimeoutMs 100
```

```
N/A
```

###### REST

```
GET /admin/brokers/configuration/{configName}/{configValue}
```

###### Java

```java
admin.brokers().updateDynamicConfiguration(configName, configValue)
```

#### Get list of dynamic configuration name
It gives list of updatable dynamic service-configuration name.

###### CLI

```
$ pulsar-admin brokers list-dynamic-config
```

```
brokerShutdownTimeoutMs
```

###### REST

```
GET /admin/brokers/configuration
```

###### Java

```java
admin.brokers().getDynamicConfigurationNames()
```

#### Get value of dynamic configurations
It gives value of all dynamic configurations stored in zookeeper

###### CLI

```
$ pulsar-admin brokers get-all-dynamic-config
```

```
brokerShutdownTimeoutMs:100
```

###### REST

```
GET /admin/brokers/configuration/values
```

###### Java

```java
admin.brokers().getAllDynamicConfigurations()
```



### Properties

A property identifies an application domain. For e.g. finance, mail,
sports etc are examples of a property. Tool allows to do CRUD operation
to manage property in Pulsar.

#### list existing properties

It lists down all existing properties in Pulsar system.  

###### CLI

```
$ pulsar-admin properties list
```

```
my-property
```

###### REST

```
GET /admin/properties
```

###### Java

```java
admin.properties().getProperties()
```


#### create property


It creates a new property in Pulsar system. For a property, you can configure admin roles (comma separated) who can manage property and clusters (comma separated) where property will be available.  

###### CLI

```
pulsar-admin properties create my-property --admin-roles admin1,admin2 --allowed-clusters cl1,cl2
```

```
N/A
```

###### REST

```
PUT /admin/properties/{property}
```

###### Java

```java
admin.properties().createProperty(property, propertyAdmin)
```


#### get property

It gets property configuration for a given existing property.  

###### CLI

```
$pulsar-admin properties get my-property
```

```json
{
    "adminRoles": [
        "admin1",
        "admin2"
    ],
    "allowedClusters": [
        "cl1",
        "cl2"
    ]
}
```

###### REST

```
GET /admin/properties/{property}
```

###### Java

```java
admin.properties().getPropertyAdmin(property)
```



#### update property

It also updates configuration of already created property. You can update admin roles and cluster information for a given existing property.  

###### CLI

```$ pulsar-admin properties update my-property --admin-roles admin-update-1,admin-update-2 --allowed-clusters cl1,cl2```

```
N/A
```

###### REST

```
POST /admin/properties/{property}
```

###### Java

```java
admin.properties().updateProperty(property, propertyAdmin)
```


#### delete property


It deletes an existing property from Pulsar system.  
###### CLI

```
$ pulsar-admin properties delete my-property
```

```
N/A
```

###### REST

```
DELETE /admin/properties/{property}
```

###### Java

```java
admin.properties().deleteProperty(property)
```




### Clusters

A cluster allows a property and its namespaces to be available in one or
more geographic locations. A cluster typically maps to a region’s
colocation name such as use, usw, etc. Tool allows to do CRUD operation
to manage cluster in Pulsar.

#### provision cluster

It provisions a new cluster in Pulsar. It also requires Pulsar super-user privileges to make sure only super-admin can perform such system level operation.  

###### CLI

```
$ pulsar-admin clusters create --url http://my-cluster.org.com:8080/ --broker-url pulsar://my-cluster.org.com:6650/ cl1
```

```
N/A
```

###### REST

```
PUT /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().createCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls))
```



#### get cluster

It gets a cluster configuration for a given existing cluster.  

###### CLI

```
$ pulsar-admin clusters get cl1
```

```json
{
    "serviceUrl": "http://my-cluster.org.com:8080/",
    "serviceUrlTls": null,
    "brokerServiceUrl": "pulsar://my-cluster.org.com:6650/",
    "brokerServiceUrlTls": null
}
```

###### REST

```
GET /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().getCluster(cluster)
```


#### update cluster

It updates cluster configuration data for a given existing cluster.  

###### CLI

```
$ pulsar-admin clusters update --url http://my-cluster.org.com:4081/ --broker-url pulsar://my-cluster.org.com:3350/ cl1
```

```
N/A
```

###### REST

```
POST /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().updateCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls))
```


#### delete cluster

It deletes an existing cluster from Pulsar system.  

###### CLI

```
$ pulsar-admin clusters delete cl1
```

```
N/A
```

###### REST

```
DELETE /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().deleteCluster(cluster)
```


#### list all clusters

It gives a list of all created clusters in Pulsar system.  

###### CLI

```
$ pulsar-admin clusters list
```

```
cl1
```

###### REST

```
GET /admin/clusters
```

###### Java

```java
admin.clusters().getClusters()
```


### Namespace

A namespace is a logical nomenclature within a property. A property may
have multiple namespaces to manage different applications under that
property.

#### create namespace

It creates a namespace for a property under a given existing cluster.  

###### CLI

```
$ pulsar-admin namespaces create test-property/cl1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().createNamespace(namespace)
```

#### get namespace

It gets created namespace policies information.  

###### CLI

```
$pulsar-admin namespaces policies test-property/cl1/ns1
```

```json
{
    "auth_policies": {
        "namespace_auth": {},
        "destination_auth": {}
    },
    "replication_clusters": [],
    "bundles_activated": true,
    "bundles": {
        "boundaries": [
            "0x00000000",
            "0xffffffff"
        ],
        "numBundles": 1
    },
    "backlog_quota_map": {},
    "persistence": null,
    "latency_stats_sample_rate": {},
    "message_ttl_in_seconds": 0,
    "retention_policies": null,
    "deleted": false
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().getPolicies(namespace)
```


#### list all namespaces under property

It gets all created namespace names under a given property.  

###### CLI

```
$ pulsar-admin namespaces list test-property
```

```
test-property/cl1/ns1
```

###### REST

```
GET /admin/namespaces/{property}
```

###### Java

```java
admin.namespaces().getNamespaces(property)
```


#### list all namespaces under cluster

It gets all namespace names under a property for a given cluster  

###### CLI

```
$ pulsar-admin namespaces list-cluster test-property/cl1
```

```
test-property/cl1/ns1
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}
```

###### Java

```java
admin.namespaces().getNamespaces(property, cluster)
```

#### delete namespace

It deletes an existing namespace  

###### CLI

```
$ pulsar-admin namespaces delete test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().deleteNamespace(namespace)
```

#### grant permission

It grants a permission to a specific client role for list of required operations such as produce and consume.  

###### CLI

```
$ pulsar-admin namespaces grant-permission --actions produce,consume --role admin10 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/permissions/{role}
```

###### Java

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions))
```


#### get permission

It shows created permission rules for a given namespace  

###### CLI

```
$ pulsar-admin namespaces permissions test-property/cl1/ns1
```

```json
{
    "admin10": [
        "produce",
        "consume"
    ]
}   
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/permissions
```

###### Java

```java
admin.namespaces().getPermissions(namespace)
```

#### revoke permission

It revokes the permission from a specific client role so, that client-role will not be able to access a given namespace.  

###### CLI

```
$ pulsar-admin namespaces revoke-permission --role admin10 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}/permissions/{role}
```

###### Java

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role)
```


#### set replication cluster

It sets replication clusters for a namespace, so Pulsar can internally replicate publish message from one colo to another colo. However, in order to set replication clusters, your namespace has to be global such as: *test-property/**global**/ns1.* It means cluster-name has to be *“global”*  

###### CLI

```
$ pulsar-admin namespaces set-clusters --clusters cl2 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/replication
```

###### Java

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters)
```    

#### get replication cluster

It gives a list of replication clusters for a given namespace.  

###### CLI

```
$ pulsar-admin namespaces get-clusters test-property/cl1/ns1
```

```
cl2
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/replication
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```

#### set backlog quota policies

Backlog quota helps broker to restrict bandwidth/storage of a namespace once it reach certain threshold limit . Admin can set this limit and one of the following action after the limit is reached.

  1.  producer_request_hold: broker will hold and not persist produce request payload

  2.  producer_exception: broker will disconnects with client by giving exception

  3.  consumer_backlog_eviction: broker will start discarding backlog messages

  Backlog quota restriction can be taken care by defining restriction of backlog-quota-type: destination_storage  

###### CLI

```
$ pulsar-admin namespaces set-backlog-quota --limit 10 --policy producer_request_hold test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy))
```

#### get backlog quota policies

It shows a configured backlog quota for a given namespace.  

###### CLI

```
$ pulsar-admin namespaces get-backlog-quotas test-property/cl1/ns1
```

```json
{
    "destination_storage": {
        "limit": 10,
        "policy": "producer_request_hold"
    }
}          
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuotaMap
```

###### Java

```java
admin.namespaces().getBacklogQuotaMap(namespace)
```




#### remove backlog quota policies  

It removes backlog quota policies for a given namespace  

###### CLI

```
$ pulsar-admin namespaces remove-backlog-quota test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```


#### set persistence policies

Persistence policies allow to configure persistency-level for all topic messages under a given namespace.

  -   Bookkeeper-ack-quorum: Number of acks (guaranteed copies) to wait for each entry, default: 0

  -   Bookkeeper-ensemble: Number of bookies to use for a topic, default: 0

  -   Bookkeeper-write-quorum: How many writes to make of each entry, default: 0

  -   Ml-mark-delete-max-rate: Throttling rate of mark-delete operation (0 means no throttle), default: 0.0  

###### CLI

```
$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```


#### get persistence policies

It shows configured persistence policies of a given namespace.  

###### CLI

```
$ pulsar-admin namespaces get-persistence test-property/cl1/ns1
```

```json
{
    "bookkeeperEnsemble": 3,
    "bookkeeperWriteQuorum": 2,
    "bookkeeperAckQuorum": 2,
    "managedLedgerMaxMarkDeleteRate": 0
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().getPersistence(namespace)
```


#### unload namespace bundle

      Namespace bundle is a virtual group of topics which belong to same namespace. If broker gets overloaded with number of bundles then this command can help to unload heavy bundle from that broker, so it can be served by some other less loaded broker. Namespace bundle is defined with it’s start and end range such as 0x00000000 and 0xffffffff.  

###### CLI

```
$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/unload
```

###### Java

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```


#### set message-ttl

It configures message’s time to live (in seconds) duration.  

###### CLI

```
$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```

#### get message-ttl

It gives a message ttl of configured namespace.  

###### CLI

```
$ pulsar-admin namespaces get-message-ttl test-property/cl1/ns1
```

```
100
```


###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```


#### split bundle

Each namespace bundle can contain multiple topics and each bundle can be served by only one broker. If bundle gets heavy with multiple live topics in it then it creates load on that broker and in order to resolve this issue, admin can split bundle using this command.

###### CLI

```
$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-property/cl1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/split
```

###### Java

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```


#### clear backlog

It clears all message backlog for all the topics those belong to specific namespace. You can also clear backlog for a specific subscription as well.  

###### CLI

```
$ pulsar-admin namespaces clear-backlog --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```


#### clear bundle backlog

It clears all message backlog for all the topics those belong to specific NamespaceBundle. You can also clear backlog for a specific subscription as well.  

###### CLI

```
$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)
```


#### set retention

Each namespace contains multiple topics and each topic’s retention size (storage size) should not exceed to a specific threshold or it should be stored till certain time duration. This command helps to configure retention size and time of topics in a given namespace.  

###### CLI

```
$ pulsar-admin set-retention --size 10 --time 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```


#### get retention

It shows retention information of a given namespace.  

###### CLI

```
$ pulsar-admin namespaces get-retention test-property/cl1/ns1
```

```json
{
    "retentionTimeInMinutes": 10,
    "retentionSizeInMB": 100
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().getRetention(namespace)
```



### Persistent

Persistent helps to access topic which is a logical endpoint for
publishing and consuming messages. Producers publish messages to the
topic and consumers subscribe to the topic, to consume messages
published to the topic.

In below instructions and commands - persistent topic format is:

```persistent://<property_name> <cluster_name> <namespace_name> <topic-name>```

#### create partitioned topic


It creates a partitioned topic under a given namespace. In order to create a partitioned topic, no of partitioned must be more than 1.  

###### CLI


```
$ pulsar-admin persistent create-partitioned-topic --partitions 2 persistent://test-property/cl1/ns1/pt1
```

```
N/A
```

###### REST

```
PUT /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().createPartitionedTopic(persistentTopic, numPartitions)
```

#### get partitioned topic

It gives metadata of created partitioned topic.  

###### CLI

```
$ pulsar-admin persistent get-partitioned-topic-metadata persistent://test-property/cl1/ns1/pt1
```

```json
{
    "partitions": 2
}
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().getPartitionedTopicMetadata(persistentTopic)
```


#### delete partitioned topic

It deletes a created partitioned topic.  

###### CLI

```
$ pulsar-admin persistent delete-partitioned-topic persistent://test-property/cl1/ns1/pt1
```

```
N/A
```

###### REST
```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().deletePartitionedTopic(persistentTopic)
```


#### Delete topic

It deletes a topic. The topic cannot be deleted if there's any active subscription or producers connected to it.  

###### CLI

```
pulsar-admin persistent delete persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}
```

###### Java

```java
admin.persistentTopics().delete(persistentTopic)
```


#### List of topics

It provides a list of persistent topics exist under a given namespace.  

###### CLI

```
$ pulsar-admin persistent list test-property/cl1/ns1
```

```
my-topic
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.persistentTopics().getList(namespace)
```

#### Grant permission

It grants permissions on a client role to perform specific actions on a given topic.  

###### CLI

```
$ pulsar-admin persistent grant-permission --actions produce,consume --role application1 persistent://test-property/cl1/ns1/tp1
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions/{role}
```

###### Java

```java
admin.persistentTopics().grantPermission(destination, role, getAuthActions(actions))
```

#### Get permission

It shows a list of client role permissions on a given topic.  

###### CLI

```
$ pulsar-admin permissions persistent://test-property/cl1/ns1/tp1
```

```json
{
    "application1": [
        "consume",
        "produce"
    ]
}
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions
```

###### Java

```java
admin.persistentTopics().getPermissions(destination)
```

#### Revoke permission

It revokes a permission which was granted on a client role.  

###### CLI

```
$ pulsar-admin persistent revoke-permission --role application1 persistent://test-property/cl1/ns1/tp1
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions/{role}
```

###### Java

```java
admin.persistentTopics().revokePermissions(destination, role)
```

#### get partitioned topic stats

It shows current statistics of a given partitioned topic.  

###### CLI

```
$ pulsar-admin persistent partitioned-stats --per-partition persistent://test-property/cl1/ns1/tp1
```

```json
{
    "msgRateIn": 4641.528542257553,
    "msgThroughputIn": 44663039.74947473,
    "msgRateOut": 0,
    "msgThroughputOut": 0,
    "averageMsgSize": 1232439.816728665,
    "storageSize": 135532389160,
    "publishers": [
        {
            "msgRateIn": 57.855383881403576,
            "msgThroughputIn": 558994.7078932219,
            "averageMsgSize": 613135,
            "producerId": 0,
            "producerName": null,
            "address": null,
            "connectedSince": null
        }
    ],
    "subscriptions": {
        "my-topic_subscription": {
            "msgRateOut": 0,
            "msgThroughputOut": 0,
            "msgBacklog": 116632,
            "type": null,
            "msgRateExpired": 36.98245516804671,
            "consumers": []
        }
    },
    "replication": {}
}          
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitioned-stats
```

###### Java

```java
admin.persistentTopics().getPartitionedStats(persistentTopic, perPartition)
```


#### Get stats


It shows current statistics of a given non-partitioned topic.

  -   **msgRateIn**: The sum of all local and replication publishers' publish rates in messages per second

  -   **msgThroughputIn**: Same as above, but in bytes per second instead of messages per second

  -   **msgRateOut**: The sum of all local and replication consumers' dispatch rates in messages per second

  -   **msgThroughputOut**: Same as above, but in bytes per second instead of messages per second

  -   **averageMsgSize**: The average size in bytes of messages published within the last interval

  -   **storageSize**: The sum of the ledgers' storage size for this topic. See

  -   **publishers**: The list of all local publishers into the topic. There can be zero or thousands

  -   **averageMsgSize**: Average message size in bytes from this publisher within the last interval

  -   **producerId**: Internal identifier for this producer on this topic

  -   **producerName**: Internal identifier for this producer, generated by the client library

  -   **address**: IP address and source port for the connection of this producer

  -   **connectedSince**: Timestamp this producer was created or last reconnected

  -   **subscriptions**: The list of all local subscriptions to the topic

  -   **my-subscription**: The name of this subscription (client defined)

  -   **msgBacklog**: The count of messages in backlog for this subscription

  -   **type**: This subscription type

  -   **msgRateExpired**: The rate at which messages were discarded instead of dispatched from this subscription due to TTL

  -   **consumers**: The list of connected consumers for this subscription

  -   **consumerName**: Internal identifier for this consumer, generated by the client library

  -   **availablePermits**: The number of messages this consumer has space for in the client library's listen queue. A value of 0 means the client library's queue is full and receive() isn't being called. A nonzero value means this consumer is ready to be dispatched messages.

  -   **replication**: This section gives the stats for cross-colo replication of this topic

  -   **replicationBacklog**: The outbound replication backlog in messages

  -   **connected**: Whether the outbound replicator is connected

  -   **replicationDelayInSeconds**: How long the oldest message has been waiting to be sent through the connection, if connected is true

  -   **inboundConnection**: The IP and port of the broker in the remote cluster's publisher connection to this broker

  -   **inboundConnectedSince**: The TCP connection being used to publish messages to the remote cluster. If there are no local publishers connected, this connection is automatically closed after a minute.  
###### CLI

```
$ pulsar-admin persistent stats persistent://test-property/cl1/ns1/tp1
```

```json
{
    "msgRateIn": 0,
    "msgThroughputIn": 0,
    "msgRateOut": 0,
    "msgThroughputOut": 0,
    "averageMsgSize": 0,
    "storageSize": 11814,
    "publishers": [
        {
            "msgRateIn": 0,
            "msgThroughputIn": 0,
            "averageMsgSize": 0,
            "producerId": 0,
            "producerName": "gq1-54-4001",
            "address": "/10.215.138.238:44458",
            "connectedSince": "2016-06-16 22:56:56.509"
        }
    ],
    "subscriptions": {
        "my-subscription": {
            "msgRateOut": 0,
            "msgThroughputOut": 0,
            "msgBacklog": 17,
            "type": "Shared",
            "msgRateExpired": 2.1771406267194497,
            "consumers": [
                {
                    "msgRateOut": 0,
                    "msgThroughputOut": 0,
                    "consumerName": "a67f7",
                    "availablePermits": 1186,
                    "address": "/10.215.166.121:35095",
                    "connectedSince": "2016-06-25 00:05:58.312"
                }
            ]
        }
    },
    "replication": {}
}
```

###### REST
```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/stats
```

###### Java

```java
admin.persistentTopics().getStats(persistentTopic)
```

#### Get detailed stats

It shows detailed statistics of a topic.

  -   **entriesAddedCounter**: Messages published since this broker loaded this topic

  -   **numberOfEntries**: Total number of messages being tracked

  -   **totalSize**: Total storage size in bytes of all messages

  -   **currentLedgerEntries**: Count of messages written to the ledger currently open for writing

  -   **currentLedgerSize**: Size in bytes of messages written to ledger currently open for writing

  -   **lastLedgerCreatedTimestamp**: time when last ledger was created

  -   **lastLedgerCreationFailureTimestamp:** time when last ledger was failed

  -   **waitingCursorsCount**: How many cursors are "caught up" and waiting for a new message to be published

  -   **pendingAddEntriesCount**: How many messages have (asynchronous) write requests we are waiting on completion

  -   **lastConfirmedEntry**: The ledgerid:entryid of the last message successfully written. If the entryid is -1, then the ledger has been opened or is currently being opened but has no entries written yet.

  -   **state**: The state of this ledger for writing. LedgerOpened means we have a ledger open for saving published messages.

  -   **ledgers**: The ordered list of all ledgers for this topic holding its messages

  -   **cursors**: The list of all cursors on this topic. There will be one for every subscription you saw in the topic stats.

  -   **markDeletePosition**: The ack position: the last message the subscriber acknowledged receiving

  -   **readPosition**: The latest position of subscriber for reading message

  -   **waitingReadOp**: This is true when the subscription has read the latest message published to the topic and is waiting on new messages to be published.

  -   **pendingReadOps**: The counter for how many outstanding read requests to the BookKeepers we have in progress

  -   **messagesConsumedCounter**: Number of messages this cursor has acked since this broker loaded this topic

  -   **cursorLedger**: The ledger being used to persistently store the current markDeletePosition

  -   **cursorLedgerLastEntry**: The last entryid used to persistently store the current markDeletePosition

  -   **individuallyDeletedMessages**: If Acks are being done out of order, shows the ranges of messages Acked between the markDeletePosition and the read-position

  -   **lastLedgerSwitchTimestamp**: The last time the cursor ledger was rolled over

  -   **state**: The state of the cursor ledger: Open means we have a cursor ledger for saving updates of the markDeletePosition.

###### CLI

```
$ pulsar-admin persistent stats-internal persistent://test-property/cl1/ns1/tp1
```

```json
{
    "entriesAddedCounter": 20449518,
    "numberOfEntries": 3233,
    "totalSize": 331482,
    "currentLedgerEntries": 3233,
    "currentLedgerSize": 331482,
    "lastLedgerCreatedTimestamp": "2016-06-29 03:00:23.825",
    "lastLedgerCreationFailureTimestamp": null,
    "waitingCursorsCount": 1,
    "pendingAddEntriesCount": 0,
    "lastConfirmedEntry": "324711539:3232",
    "state": "LedgerOpened",
    "ledgers": [
        {
            "ledgerId": 324711539,
            "entries": 0,
            "size": 0
        }
    ],
    "cursors": {
        "my-subscription": {
            "markDeletePosition": "324711539:3133",
            "readPosition": "324711539:3233",
            "waitingReadOp": true,
            "pendingReadOps": 0,
            "messagesConsumedCounter": 20449501,
            "cursorLedger": 324702104,
            "cursorLedgerLastEntry": 21,
            "individuallyDeletedMessages": "[(324711539:3134‥324711539:3136], (324711539:3137‥324711539:3140], ]",
            "lastLedgerSwitchTimestamp": "2016-06-29 01:30:19.313",
            "state": "Open"
        }
    }
}
```


###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/internalStats
```

###### Java

```java
admin.persistentTopics().getInternalStats(persistentTopic)
```


#### Peek messages

It peeks N messages for a specific subscription of a given topic.  

###### CLI

```
$ pulsar-admin persistent peek-messages --count 10 --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```          
Message ID: 315674752:0  
Properties:  {  "X-Pulsar-publish-time" : "2015-07-13 17:40:28.451"  }
msg-payload
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/position/{messagePosition}
```


###### Java

```java
admin.persistentTopics().peekMessages(persistentTopic, subName, numMessages)
```


#### Skip messages

It skips N messages for a specific subscription of a given topic.  

###### CLI

```
$ pulsar-admin persistent skip --count 10 --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip/{numMessages}
```

###### Java

```java
admin.persistentTopics().skipMessages(persistentTopic, subName, numMessages)
```

#### Skip all messages

It skips all old messages for a specific subscription of a given topic.  

###### CLI

```
$ pulsar-admin persistent skip-all --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip_all
```

###### Java

```java
admin.persistentTopics().skipAllMessages(persistentTopic, subName)
```

#### Expire messages

It expires messages which are older than given expiry time (in seconds) for a specific subscription of a given topic.

###### CLI

```
$ pulsar-admin persistent expire-messages --subscription my-subscription --expireTime 120 persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/expireMessages/{expireTimeInSeconds}
```

###### Java

```java
admin.persistentTopics().expireMessages(persistentTopic, subName, expireTimeInSeconds)
```

#### Expire all messages 

It expires messages which are older than given expiry time (in seconds) for all subscriptions of a given topic.

###### CLI

```
$ pulsar-admin persistent expire-messages-all-subscriptions --expireTime 120 persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/all_subscription/expireMessages/{expireTimeInSeconds}
```

###### Java

```java
admin.persistentTopics().expireMessagesForAllSubscriptions(persistentTopic, expireTimeInSeconds)
```



#### Reset cursor

It resets a subscription’s cursor position back to the position which was recorded X minutes before. It essentially calculates time and position of cursor at X minutes before and resets it at that position.  

###### CLI

```
$ pulsar-admin persistent reset-cursor --subscription my-subscription --time 10 persistent://test-property/pstg-gq1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/resetcursor/{timestamp}
```

###### Java

```java
admin.persistentTopics().resetCursor(persistentTopic, subName, timestamp)
```


#### Lookup of topic


It locates broker url which is serving the given topic.  

###### CLI

```
$ pulsar-admin persistent lookup persistent://test-property/pstg-gq1/ns1/my-topic
```

```
"pulsar://broker1.org.com:4480"
```

###### REST
```
GET http://<broker-url>:<port>/lookup/v2/destination/persistent/{property}/{cluster}/{namespace}/{dest}
(\* this api serves by “lookup” resource and not “persistent”)
```

###### Java

```java
admin.lookups().lookupDestination(destination)
```

#### Get subscriptions

It shows all subscription names for a given topic.  

###### CLI

```
$ pulsar-admin persistent subscriptions persistent://test-property/pstg-gq1/ns1/my-topic
```

```
my-subscription
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscriptions
```

###### Java

```java
admin.persistentTopics().getSubscriptions(persistentTopic)
```

#### unsubscribe

It can also help to unsubscribe a subscription which is no more processing further messages.  

###### CLI

```
$pulsar-admin persistent unsubscribe --subscription my-subscription persistent://test-property/pstg-gq1/ns1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}
```

###### Java

```java
admin.persistentTopics().deleteSubscription(persistentTopic, subName)
```

### Namespace isolation policy

#### create/update ns-isolation policy

It creates a namespace isolation policy.

  -   auto-failover-policy-params: comma separated name=value auto failover policy parameters

  -   auto-failover-policy-type: auto failover policy type name ['min_available']

  -   namespaces: comma separated namespaces-regex list

  -   primary: comma separated primary-broker-regex list

  -   secondary: comma separated secondary-broker-regex list  

###### CLI

```
$ pulsar-admin ns-isolation-policy --auto-failover-policy-params min_limit=0 --auto-failover-policy-type min_available --namespaces test-property/cl1/ns.*|test-property/cl1/test-ns*.* --secondary broker2.* --primary broker1.* cl1 ns-is-policy1
```

```
N/A
```

###### REST

```
POST /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java
```java
admin.clusters().createNamespaceIsolationPolicy(clusterName, policyName, namespaceIsolationData);
```


#### get ns-isolation policy


It shows a created namespace isolation policy.  

###### CLI

```
$ pulsar-admin ns-isolation-policy get cl1 ns-is-policy1
```

```json
{
    "namespaces": [
        "test-property/cl1/ns.*|test-property/cl1/test-ns*.*"
    ],
    "primary": [
        "broker1.*"
    ],
    "secondary": [
        "broker2.*"
    ],
    "auto_failover_policy": {
        "policy_type": "min_available",
        "parameters": {
            "min_limit": "0"
        }
    }
}
```

###### REST
```
GET /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java

```java
admin.clusters().getNamespaceIsolationPolicy(clusterName, policyName)
```


#### delete ns-isolation policy

It deletes a namespace isolation policy.  

###### CLI

```
$ pulsar-admin ns-isolation-policy delete ns-is-policy1
```

```
N/A
```

###### REST

```
DELETE /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java

```java
admin.clusters().deleteNamespaceIsolationPolicy(clusterName, policyName)
```


#### list all ns-isolation policy


It shows all namespace isolation policies served by a given cluster.  

###### CLI

```
$ pulsar-admin ns-isolation-policy list cl1
```

```json
{
    "ns-is-policy1": {
        "namespaces": [
            "test-property/cl1/ns.*|test-property/cl1/test-ns*.*"
        ],
        "primary": [
            "broker1.*"
        ],
        "secondary": [
            "broker2.*"
        ],
        "auto_failover_policy": {
            "policy_type": "min_available",
            "parameters": {
                "min_limit": "0"
            }
        }
    }
}
```

###### REST
```
GET /admin/clusters/{cluster}/namespaceIsolationPolicies
```

###### Java

```java
admin.clusters().getNamespaceIsolationPolicies(clusterName)
```


### Resource-Quotas

#### set namespace resource quota

It sets customize quota information for a given namespace bundle.  

###### CLI

```
$ pulsar-admin resource-quotas set --bandwidthIn 10 --bandwidthOut 10 --bundle 0x00000000_0xffffffff --memory 10 --msgRateIn 10 --msgRateOut 10 --namespace test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().setNamespaceBundleResourceQuota(namespace, bundle, quota)
```

#### get namespace resource quota

It shows configured resource quota information.  

###### CLI

```
$ pulsar-admin resource-quotas get  --bundle 0x00000000_0xffffffff --namespace test-property/cl1/my-topic
```

```json
{
    "msgRateIn": 80.40352101165782,
    "msgRateOut": 132.58187392933146,
    "bandwidthIn": 144273.8819600397,
    "bandwidthOut": 234497.9190227951,
    "memory": 199.91739142481595,
    "dynamic": true
}          
```

###### REST
```
GET /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().getNamespaceBundleResourceQuota(namespace, bundle)
```

#### reset namespace resource quota

It again reverts back the customized resource quota and sets back default resource_quota.  

###### CLI

```
$ pulsar-admin resource-quotas reset-namespace-bundle-quota --bundle 0x00000000_0xffffffff --namespace test-property/cl1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().resetNamespaceBundleResourceQuota(namespace, bundle)
```


Pulsar additional tools
---

### Pulsar client tool

As we know that Pulsar provides Java api to produce and consume messages on a given topic. However, Pulsar also provides CLI utility which also helps to produce and consume messages on a topic.

In your terminal, go to below directory to play with client tool.

```$ $PULSAR_HOME/bin```

```$ ./pulsar-client --help```

#### produce message command
<table>
  <tbody>
  <tr>
      <td colspan="2">```pulsar-client produce```</td>   
    </tr>
    <tr>
      <th>options</th>
      <th>description</th>   
    </tr>
    <tr>
      <td>```-f, --files```</td>
      <td>```Comma separated file paths to send. Cannot be used with -m. Either -f or -m must be provided```</td>
    </tr>
    <tr>
      <td>```-m, --messages```</td>
      <td>```Comma separted string messages to send. Cannot be used with -f. Either -m or -f must be provided```</td>
    </tr>
    <tr>
      <td>```-n, --num-produce```</td>
      <td>```Number of times to send message(s), Default: 1```</td>
    </tr>
    <tr>
      <td>```-r, --rate```</td>
      <td>```Rate (in msg/sec) at which to produce. Value of 0 will produce messages as fast as possible, Default: 0.0```</td>
    </tr>
<table>

#### consume message command
<table>
  <tbody>
  <tr>
      <td colspan="2">```pulsar-client consume```</td>   
    </tr>
    <tr>
      <th>options</th>
      <th>description</th>   
    </tr>
    <tr>
      <td>```--hex```</td>
      <td>```Display binary messages in hex, Default: false```</td>
    </tr>
    <tr>
      <td>```-n, --num-messages```</td>
      <td>```Number of messages to consume, Default: 1```</td>
    </tr>
    <tr>
      <td>```-r, --rate```</td>
      <td>```Rate (in msg/sec) at which to consume. Value of 0 will consume messages as fast as possible, Default: 0.0```</td>
    </tr>
    <tr>
      <td>```-s, --subscription-name```</td>
      <td>```Subscription name```</td>
    </tr>
    <tr>
      <td>```-t, --subscription-type```</td>
      <td>```Subscription type: Exclusive, Shared, Failover, Default: Exclusive```</td>
    </tr>
<table>
