# PIP-52: Message dispatch throttling relative to publish rate

* **Status**: Implemented
 * **Author**: [Rajan Dhabalia](https://github.com/rdhabalia)
 * **Pull Request**: [#5797](https://github.com/apache/pulsar/pull/5797)

## Motivation
Producers and consumers can interact with broker on high volumes of data. This can monopolize broker resources and cause network saturation, which can have adverse effects on other topics owned by that broker. Throttling based on resource consumption can protect against these issues and it can play an important role for large multi-tenant clusters where a small set of clients using high volumes of data can easily degrade performance of other clients. 

With [PIP-3](https://github.com/apache/pulsar/wiki/PIP-3:-Message-dispatch-throttling), Pulsar broker already supports to configure dispatch rate-limiting for a given topic. Dispatch-throttling feature allows user to configure absolute dispatch rate based on current publish-rate for a given topic or subscriber, and broker will make sure to dispatch only configured number of messages to the consumers regardless current publish-rate or backlog on that topic. 

Current dispatch-rate limiting doesn't consider change in publish-rate so, increasing publish-rate on the topic might be larger than configured dispatch-rate which will cause backlog on the topic and consumers will never be able to catch up the backlog unless user again reconfigured the dispatch-rate based on current publish-rate. Reconfiguring dispatch-rate based on publish-rate requires human interaction and monitoring. Therefore, we need a mechanism to configure dispatch rate relative to the current publish-rate on the topic.

## Dispatch rate-limiting relative to publish rate

Dispatch rate-limiting relative to current publish-rate allows broker to adjust dispatch-rate dynamically based on current publish-rate which still throttles dispatch but additionally it gives an opportunity to consumers to catch up with backlog.

User should be able to configure relative dispatch rate-limiting using the same distpatch-rate-limit admin-api but by passing additional relative-throttling flag in input. If user always wants to dispatch 200 messages more than current publish rate then user will set dispatch-rate as 200 messages with additional flag for relative throttling. Relative throttling will make sure that broker will always dispatch 200 messages more than current published rate. For example: if current published rate is 1000 then broker will set dispatch rate-limiting value to 1200  (1000 (current published-rate) + 200 (dispatch rate-limit)) and if published-rate increases from 1000 to 5000 then broker will dynamically update dispatch rate-limiting value to 5200 (5000 (current published-rate) + 200 (dispatch rate-limit)). However, if publish rate decreases then broker will gradually try to decrease dispatch rate-limiting by considering previous sampling.

### Broker changes
In order to support, relative dispatch rate-limiting, broker needs to know the latest publish rate on the topic and periodically adjust permits for dispatcher’s rate-limiter accordingly. Broker will reuse existing dispatch rate-limiting framework and will add relative dispatch throttling enhancement on top of it.

 
#### Namespace throttling configuration
(a) With below configuration, broker will try to dispatch 1000 messages more than current publish-rate for each topic under the given namespace.
```
pulsar-admin namespaces <property/cluster/namespace> set-dispatch-rate --msg-dispatch-rate 1000 --relative-to-publish-rate
```

And with below configuration, broker will try to dispatch 1000 messages more than current publish-rate for each subscriber of topic under the given namespace
```
pulsar-admin namespaces <property/cluster/namespace> set-subscription-dispatch-rate --msg-dispatch-rate 1000 --relative-to-publish-rate
```

(b) With below configuration, broker will try to dispatch 1000 bytes more than current publish-rate for each topic under the given namespace.
```
pulsar-admin namespaces <property/cluster/namespace> set-dispatch-rate --byte-dispatch-rate 1000 --relative-to-publish-rate
```

And with below configuration, broker will try to dispatch 1000 bytes more than current publish-rate for each subscriber of topic under the given namespace
```
pulsar-admin namespaces <property/cluster/namespace> set-subscription-dispatch-rate --byte-dispatch-rate 1000 --relative-to-publish-rate
```

By default, relative throttling will be disabled for dispatch rate-limiting and it can be enabled by passing `relative-to-publish-rate` flag in api.
