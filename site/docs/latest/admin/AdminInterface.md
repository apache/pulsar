---
title: The Pulsar admin interface
tags: [admin, cli, rest, java]
---

The Pulsar admin interface enables you to manage all of the important entities in a Pulsar {% popover instance %}, such as {% popover properties %}, {% popover topics %}, and {% popover namespaces %}.

You can currently interact with the admin interface via:

1. Making HTTP calls against the admin [REST API](../../reference/RestApi) provided by Pulsar {% popover brokers %}.
1. The `pulsar-admin` CLI tool, which is available in the `bin` folder of your [Pulsar installation](../../getting-started/LocalCluster):

    ```shell
    $ bin/pulsar-admin
    ```

    Full documentation for this tool can be found in the [Pulsar command-line tools](../../reference/CliTools#pulsar-admin) doc.

1. A Java client interface.

{% include message.html id="admin_rest_api" %}

In this document, examples from each of the three available interfaces will be shown.

## Admin setup

{% include explanations/admin-setup.md %}

## Managing clusters

{% include explanations/cluster-admin.md %}

## Managing brokers

{% include explanations/broker-admin.md %}

## Managing Properties

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

{% endpoint GET /admin/properties %}

###### Java

```java
admin.properties().getProperties()
```


#### create property


It creates a new property in Pulsar system. For a property, you can configure admin roles (comma separated) who can manage property and clusters (comma separated) where property will be available.  

###### CLI

```shell
$ pulsar-admin properties create my-property \
  --admin-roles admin1,admin2 \
  --allowed-clusters cl1,cl2
```

```
N/A
```

###### REST

{% endpoint PUT /admin/properties/:property %}

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

{% endpoint GET /admin/properties/:property %}

###### Java

```java
admin.properties().getPropertyAdmin(property)
```



#### update property

It also updates configuration of already created property. You can update admin roles and cluster information for a given existing property.  

###### CLI

```shell
$ pulsar-admin properties update my-property \
  --admin-roles admin-update-1,admin-update-2 \
  --allowed-clusters cl1,cl2
```

###### REST

{% endpoint POST /admin/properties/:property %}

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

{% endpoint DELETE /admin/properties/:property %}

###### Java

```java
admin.properties().deleteProperty(property);
```

## Managing namespaces

{% include explanations/namespace-admin.md %}

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

## Managing persistent topics

Persistent helps to access topic which is a logical endpoint for
publishing and consuming messages. Producers publish messages to the
topic and consumers subscribe to the topic, to consume messages
published to the topic.

In below instructions and commands - persistent topic format is:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

{% include explanations/persistent-topic-admin.md %}

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

## Managing non-peristent topics

Non-persistent can be used in applications that only want to consume real time published messages and 
do not need persistent guarantee that can also reduce message-publish latency by removing overhead of 
persisting messages.

{% include explanations/non-persistent-topic-admin.md %}


## Managing partitioned topics

Partitioned topic is actually implemented as N internal topics, where N is the number of partitions.
When publishing messages to a partitioned topic, each message is routed to one of several brokers.
The distribution of partitions across brokers is handled automatically by Pulsar.

In below instructions and commands - persistent topic format is:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

{% include explanations/partitioned-topics.md %}


### Partitioned topics

{% include explanations/partitioned-topic-admin.md %}
