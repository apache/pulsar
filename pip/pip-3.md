# PIP-3: Message dispatch throttling

* **Status**: Implemented
 * **Author**: [Rajan Dhabalia](https://github.com/rdhabalia)
 * **Pull Request**: [#634](https://github.com/apache/incubator-pulsar/pull/634)

## Motivation
Producers and consumers can interact with broker on high volumes of data. This can monopolize broker resources and cause network saturation, which can have adverse effects on other topics owned by that broker. Throttling based on resource consumption can protect against these issues and it can play important role for large multi-tenant clusters where a small set of clients using high volumes of data can easily degrade performance of other clients.
## Message throttling
### Consumer: 
Sometimes, namespace with large message backlog can continuously drain messages with multiple connected consumers, which can easily over utilize broker’s bandwidth. Therefore, we can configure dispatch-rate for that namespace which will enforce throttling while dispatching messages to the consumers. Throttling limit can be configured at namespace level by defining dispatch-rate threshold which will apply to each of the topic under that namespace.

By default, broker does not throttle message dispatching for any namespace unless cluster is configured with default throttling-limit or namespace has been configured as a part of namespace policy in a cluster. We can configure throttling limit for a specific namespace using an [admin-api](https://pulsar.incubator.apache.org/docs/latest/admin-api/namespaces/).

### Producer: 
Broker already has capability to throttle number of publish messages by reading only fixed number of in flight messages per client connection, which can protect broker if client tries to publish significantly large number of messages.

## Broker changes:

### Throttling limit per topic
After configuring dispatch rate-limit, each topic can dispatch with in that rate-limit . If a topic is configured with rate-limiting of 10 message per second then broker dispatches only 10 messages per second for that topic. 
### Throttling limit shared across all subscriptions of the topic
Message-rate limit applies across all the subscriptions for a loaded topic. Therefore, broker initializes rate-limiter with configured number of limit which will be shared across all the subscriptions of the topic while dispatching messages. So, subscription reads available messages only if rate-limiter has permits available else subscription backs off and schedule next read after a second. 
### Throttling for only: subscription with backlog
If subscription doesn’t have backlog and it has already caught up then it reads published messages from the broker’s cache and it doesn’t have to read from the bookie. Therefore, broker doesn’t apply message-rate limiting for the subscribers which are already caught up with producers and consuming messages without building a backlog.
### Throttling limit: Message-rate Vs Byte-rate
Throttling limit can be defined in two ways:

**Message-rate:**
Message dispatching can be throttled by total number of message per duration. If message-rate is configured 100/second then broker dispatches only 100 messages per second.

**Byte-rate:**
Message dispatching can be throttled by total number of bytes per duration. If byte-rate is configured 1MB/second then broker dispatches messages with total size of 1 MB per second.

If both the limits are configured for a topic then broker applies both the limit and throttles based on whichever threshold reaches first.

 

## Cluster throttling configuration
By default, broker does not throttle message dispatching for any of the topics. However, if we want to uniformly distribute broker’s resources across all the topics then, we can configure default message-rate at cluster level and it will be immediately effective dynamically after configuring it . This default dispatch-rate in a cluster will apply to all the topics of all the brokers serving in that cluster. However, namespace with already configured throttling will override cluster’s default limit while dispatching messages for all the topics under that namespace.

Following configuration forces every broker in the cluster to dispatch only 1000 messages per second for each topic served by the broker. 
```shell
Pulsar-admin brokers update-dynamic-config --config dispatchThrottlingRatePerTopicInMsg --value 1000
```

Following configuration forces every broker in the cluster to dispatch only 1000 bytes per second for each topic served by the broker. 
```shell
Pulsar-admin brokers update-dynamic-config --config dispatchThrottlingRatePerTopicInByte --value 1000
```

By default, value of both the configurations is 0 which disables throttling in that cluster.


## Namespace throttling configuration
We can always override the default throttling-limit for namespace that need a higher or lower dispatch-rate. We can configure throttling-limit for all the topics under a specific namespace and it will be immediately effective dynamically after configuring it. Also, throttling-limit for a namespace will be configured per cluster that gives flexibility to configure specific dispatch-rate for the namespace in each cluster. Dispatch-rate configuration for each namespace will be stored under `/admin/policies`into global-zookeeper.

Following configuration forces broker to dispatch only 1000 messages per second for each topic under the given namespace. 
```shell
pulsar-admin namespaces <property/cluster/namespace> set-dispatch-rate --msg-dispatch-rate 1000

```

Following configuration forces broker to dispatch only 1000 bytes per second for each topic under the given namespace. 
```shell
pulsar-admin namespaces <property/cluster/namespace> set-dispatch-rate --byte-dispatch-rate 1000

```

By default, value of both the configurations is 0 which disables dispatch-throttling for the namespace.
 
## Alternate approach:

### Dispatch throttling per subscriber:
In above approach, broker does message dispatching throttling per namespace. However, there could be a high possibility that specific subscriber of the topic has a large backlog and over consuming bandwidth of the broker. Therefore, other topics or subscribers under the same namespace can be impacted and suffer due to namespace level throttling. In that case, we can provide an option to configure subscriber level throttling by storing subscriber configuration (under `/managed-ledger/property/cluster/ns/persistent/topic/subscriber/configuration`) into zookeeper. However, it can create an administrative complexity to manage configurations for every topic and subscriber.

### Dispatch throttling per topic:
As described in [#402](https://github.com/apache/incubator-pulsar/issues/402#issuecomment-302434483), we can define rate limiting policy that maps to the list of regex which can match to the topics for which we want to define throttling. This approach can give more granular control by throttling on topic level. However, on a long run it might be complex or difficult to manage rate limiting policies for large number of topics.
