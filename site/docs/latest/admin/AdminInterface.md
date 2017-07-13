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

### Clusters

{% include explanations/cluster-admin.md %}

## Managing namespaces

{% include explanations/namespace-admin.md %}

## Managing peristent topics

Persistent helps to access topic which is a logical endpoint for
publishing and consuming messages. Producers publish messages to the
topic and consumers subscribe to the topic, to consume messages
published to the topic.

In below instructions and commands - persistent topic format is:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

{% include explanations/partitioned-topics.md %}

## Permissions

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

## Partitioned topics

{% include explanations/partitioned-topic-admin.md %}
