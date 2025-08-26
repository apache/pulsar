# PIP-48: hierarchical admin api

- Status: Draft
- Author: [Florentin Dubois](https://github.com/FlorentinDUBOIS) ([@FlorentinDUBOIS](https://twitter.com/FlorentinDUBOIS)), [Steven Le Roux](https://github.com/StevenLeRoux) ([@GwinizDu](https://twitter.com/GwinizDu)) 
- Pull request:
- Mailing list discussion:
- Release:

## Motivation

The current pulsar admin APIs (v2,v3) inherits the historical structure, managing entities as prefix to the api's route path in a flat fashion.
Also, Pulsar evoled to the ability to manage multiple clusters.

For example to administrate a namespace, we used the following route `v2/namespaces/:tenant-id/:namespace-id`. 
This could be confusing, because we intend to manipulate a namespace under a tenant scope, which still requires here to give the tenant identifier in addition of the namespace identifier.

This proposal aims to 
- offer a more hierarchical routing approach reflecting the Pulsar semantic
- proposes to officially name an ensemble of Pulsar cluster : a `Constellation`
- to simplify the user experience for : 
  - Topic management between persistent and non-persistent.
  - cluster management inside a Pulsar Constellation


## Current admin api

Currently, we have the following admin api to control pulsar deployments.

> Scope is on what the api deals with to execute the asked action.

| path                 | description                        | scope                            |
| -------------------- | ---------------------------------- | -------------------------------- |
| `v2/functions`       | manage pulsar functions            | tenant, namespace, function      |
| `v2/clusters`        | manage clusters                    | cluster                          |
| `v2/resource-quotas` | manage quota on resource           | tenant, namespace                |
| `v2/tenants`         | manage tenants                     | tenant                           |
| `v2/namespaces`      | manage namespaces                  | tenant, namespace                |
| `v2/non-persistent`  | manage non-persistent topic        | tenant, namespace, topic         |
| `v2/persistent`      | manage persistent topic            | tenant, namespace, topic         |
| `v2/schemas`         | manage schema on registry          | tenant, namespace, topic, schema |
| `v2/brokers`         | manage brokers                     | cluster, broker                  |
| `v2/broker-stats`    | retrieve broker statistics         | cluster, broker                  |
| `v2/worker`          | manage worker                      | cluster, worker                  |
| `v2/worker-stats`    | retrieve worker statistics         | cluster, worker                  |
| `v2/bookies`         | retrieve bookies information       | cluster, bookie                  |
| `v3/sink`            | manage pulsar-io sink connectors   | tenant, namespace, connector     |
| `v3/sinks`           | manage pulsar-io sink connectors   | tenant, namespace, connector     |
| `v3/source`          | manage pulsar-io source connectors | tenant, namespace, connector     |
| `v3/sources`         | manage pulsar-io source connectors | tenant, namespace, connector     |

## Proposed changes

We would like to propose new `v4` admin api which will have two level of reading.

The first one is the `broker-level` which means all information linked to the pulsar broker's instance. The second one is the `constellation-level` which means the administration of multiple pulsar's cluster instances.

> The "constellation" word is used to differentiate a pulsar's cluster instance and the management of multiple pulsar's cluster instances. 

### Broker

The api at `broker` level will expose information of the broker, this aims to enhance the observability and operational tasks around the pulsar's broker.

| path               | description                                                  | scope          |
| ------------------ | ------------------------------------------------------------ | -------------- |
| `v4/metrics`       | broker metrics (both broker and worker) in prometheus format | broker, worker |
| `v4/broker/status` | retrieve broker status                                       | broker         |
| `v4/broker/stats`  | replacement of broker-stats                                  | broker         |
| `v4/worker/status` | retrieve worker status                                       | worker         |
| `v4/worker/stats`  | replacement of worker-stats                                  | worker         |

### Constellation

The api at `constellation` level will expose management of pulsar's cluster instances, tenants, namespaces, schemas...

| path                                                                     | description                                                                                                                                                                                                                                                                                               | scope                                           |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `v4/clusters`                                                            | List and create cluster instances                                                             | constellation                                   |
| `v4/clusters/:cluster-id`                                                | manage cluster instance `cluster-id`. (*)                                                     | cluster                                         |
| `v4/clusters/:cluster-id/tenants`                                        | list active tenants on this cluster                                                           | cluster                                         |
| `v4/clusters/:cluster-id/workers`                                        | manage workers of the `cluster-id`                                                            | cluster, worker                                 |
| `v4/clusters/:cluster-id/brokers`                                        | manage brokers of the `cluster-id`                                                            | cluster, broker                                 |
| `v4/clusters/:cluster-id/bookies`                                        | manage bookies of the `cluster-id`                                                            | cluster, bookie                                 |
| `v4/tenants`                                                             | manage tenants                                                                                | constellation, tenant                           |
| `v4/tenants/:tenant-id`                                                  | manage the tenant `tenant-id`                                                                 | constellation, tenant                           |
| `v4/tenants/:tenant-id/namespaces`                                       | manage namespaces of the tenant `tenant-id`                                                   | constellation, tenant, namespace                |
| `v4/tenants/:tenant-id/namespaces/:namespace-id`                         | manage the namespace `namespace-id` of the tenant `tenant-id`                                 | constellation, tenant, namespace                |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/resource-quotas`         | manage quotas on resources of the tenant `tenant-id` and namespace `namespace-id`             | constellation, tenant, namespace                |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/topics`                  | manage topics of the tenant `tenant-id` and namespace `namespace-id`                          | constellation, tenant, namespace, topic         |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/topics/:topic-id/schema` | manage schema of the topic `topic-id` on the tenant `tenant-id` and namespace `namespace-id`  | constellation, tenant, namespace, topic, schema |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/functions`               | manage functions of the tenant `tenant-id` and namespace `namespace-id`                       | constellation, tenant, namespace, function      |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/sinks`                   | manage sinks of the tenant `tenant-id` and namespace `namespace-id`                           | constellation, tenant, namespace, connector     |
| `v4/tenants/:tenant-id/namespaces/:namespace-id/sources`                 | manage sources of the tenant `tenant-id` and namespace `namespace-id`                         | constellation, tenant, namespace, connector     |


(*) 
Currently, Pulsar would always answer with local brokers, whatever the cluster given as parameters. If unintended requests (aka cluster A got a request for the cluster B), the cluster would answered with an http forward status (aka 301) to redirect to the right cluster by look up in the configuration store the service url. This rule is implied for sub-paths. This way we ensure consistency among brokers, using the local zookeeper where they're registered.
