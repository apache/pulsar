---
id: administration-isolation-broker
title: Isolate brokers
sidebar_label: "Isolate brokers"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


In Pulsar, when namespaces (more specifically, namespace bundles) are assigned dynamically to brokers, the namespace isolation policy limits the set of brokers that can be used for assignment. Before topics are assigned to brokers, you can set the namespace isolation policy with a primary or a secondary regex to select desired brokers.

To set a namespace isolation policy for a broker cluster, you can use one of the following methods. 

````mdx-code-block
<Tabs 
  defaultValue="Pulsar-admin CLI"
  values={[{"label":"Pulsar-admin CLI","value":"Pulsar-admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java admin API","value":"Java admin API"}]}>

<TabItem value="Pulsar-admin CLI">

```shell
pulsar-admin ns-isolation-policy set options
```

For more information about the command `pulsar-admin ns-isolation-policy set options`, see [Pulsar admin docs](https://pulsar.apache.org/tools/pulsar-admin/).

**Example**

```shell
bin/pulsar-admin ns-isolation-policy set \
--auto-failover-policy-type min_available \
--auto-failover-policy-params min_limit=1,usage_threshold=80 \
--namespaces my-tenant/my-namespace \
--primary 10.193.216.*  my-cluster policy-name
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/:namespace/:tenant/:namespace|operation/createNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java admin API">

For how to set namespace isolation policy using Java admin API, see [code](https://github.com/apache/pulsar/blob/master/pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/NamespacesImpl.java#L251).

</TabItem>

</Tabs>
````


:::tip

To guarantee all the data that belongs to a namespace is stored in desired bookies, you can isolate the data of the namespace into user-defined groups of bookies. See [configure bookie affinity groups](administration-isolation-bookie.md#configure-bookie-affinity-groups) for more details.

:::