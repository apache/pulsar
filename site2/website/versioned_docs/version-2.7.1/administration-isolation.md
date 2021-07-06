---
id: version-2.7.1-administration-isolation
title: Pulsar isolation
sidebar_label: Pulsar isolation
original_id: administration-isolation
---

In an organization, a Pulsar instance provides services to multiple teams. When organizing the resources across multiple teams, you want to make a suitable isolation plan to avoid the resource competition between different teams and applications and provide high-quality messaging service. In this case, you need to take resource isolation into consideration and weigh your intended actions against expected and unexpected consequences.

To enforce resource isolation, you can use the Pulsar isolation policy, which allows you to allocate resources (**broker** and **bookie**) for the namespace.

## Broker isolation

In Pulsar, when namespaces (more specifically, namespace bundles) are assigned dynamically to brokers, the namespace isolation policy limits the set of brokers that can be used for assignment. Before topics are assigned to brokers, you can set the namespace isolation policy with a primary or a secondary regex to select desired brokers.

You can set a namespace isolation policy for a cluster using one of the following methods. 

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

```
pulsar-admin ns-isolation-policy set options
```

For more information about the command `pulsar-admin ns-isolation-policy set options`, see [here](https://pulsar.apache.org/tools/pulsar-admin/).

**Example**

```shell
bin/pulsar-admin ns-isolation-policy set \
--auto-failover-policy-type min_available \
--auto-failover-policy-params min_limit=1,usage_threshold=80 \
--namespaces my-tenant/my-namespace \
--primary 10.193.216.*  my-cluster policy-name
```

<!--REST API-->

[PUT /admin/v2/namespaces/{tenant}/{namespace}](https://pulsar.apache.org/admin-rest-api/?version=2.7.0&apiversion=v2#operation/createNamespace)

<!--Java admin API-->

For how to set namespace isolation policy using Java admin API, see [here](https://github.com/apache/pulsar/blob/master/pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/NamespacesImpl.java#L251).

<!--END_DOCUSAURUS_CODE_TABS-->

## Bookie isolation

A namespace can be isolated into user-defined groups of bookies, which guarantees all the data that belongs to the namespace is stored in desired bookies. The bookie affinity group uses the BookKeeper [rack-aware placement policy](https://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/client/EnsemblePlacementPolicy.html) and it is a way to feed rack information which is stored as JSON format in znode.

You can set a bookie affinity group using one of the following methods.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

```
pulsar-admin namespaces set-bookie-affinity-group options
```

For more information about the command `pulsar-admin namespaces set-bookie-affinity-group options`, see [here](https://pulsar.apache.org/tools/pulsar-admin/).

**Example**

```shell
bin/pulsar-admin namespaces set-bookie-affinity-group public/default \
--primary-group group-bookie1
```

<!--REST API-->

[POST /admin/v2/namespaces/{tenant}/{namespace}/persistence/bookieAffinity](https://pulsar.apache.org/admin-rest-api/?version=2.7.0&apiversion=v2#operation/setBookieAffinityGroup)

<!--Java admin API-->

For how to set bookie affinity group for a namespace using Java admin API, see [here](https://github.com/apache/pulsar/blob/master/pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/NamespacesImpl.java#L1164).

<!--END_DOCUSAURUS_CODE_TABS-->
