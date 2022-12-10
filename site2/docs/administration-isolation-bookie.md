---
id: administration-isolation-bookie
title: Isolate bookies
sidebar_label: "Isolate bookies"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


Isolating bookies equals isolating message storage, which is a data storage mechanism that provides isolation and safety for specific topics.

Bookie isolation is controlled by BookKeeper clients. For Pulsar, there are two kinds of BookKeeper clients to read and write data.
*  BookKeeper clients on the broker side: Pulsar brokers use these BookKeeper clients to read and write topic messages.
*  BookKeeper clients on the bookie auto-recovery side:
   * The bookie auditor checks whether ledger replicas fulfill the configured isolation policy;
   * The bookie replication worker writes ledger replicas to target bookies according to the configured isolation policy.

To isolate bookies, you need to complete the following tasks.
1. Select a [data isolation policy](#understand-bookie-data-isolation-policy) based on your requirements.
2. [Enable the policy on BookKeeper clients](#enable-bookie-data-placement-policy).
3. [Configure the policy on bookie instances](#configure-data-placement-policy-on-bookie-instances).


## Understand bookie data isolation policy

Bookie data isolation policy is built on top of the existing BookKeeper rack-aware placement policy. The "rack" concept can be anything, for example, racks, regions, availability zones. It writes the configured isolation policy into the metadata store. Both BookKeeper clients on the broker and bookie auto-recovery side read the configured isolation policy from the metadata store and apply it when choosing bookies to store messages.

BookKeeper provides three kinds of data isolation policies for disaster tolerance.
* Rack-aware placement policy (default)
* Region-aware placement policy
* Zone-aware placement policy

:::tip

* Both [rack-aware placement policy](#rack-aware-placement-policy) and [region-aware placement policy](#region-aware-placement-policy) can be used in all kinds of deployments where racks are a subset of a region. The major difference between the two policies is:
  * With `RackawareEnsemblePlacementPolicy` configured, the BookKeeper client chooses bookies from different **racks** to reduce the single point of failure. If there is only one rack available, the policy falls back on choosing a random bookie across available ones.
  * With `RegionAwareEnsemblePlacementPolicy` configured, the BookKeeper client chooses bookies from different **regions**; for the selected region, it chooses bookies from different racks if more than one ensemble falls into the same region.

* Zone-aware placement policy (`ZoneAwareEnsemblePlacementPolicy`) can be used in a public cloud infrastructure where Availability Zones (AZs) are isolated locations within the data center regions that public cloud services originate from and operate in.

:::

### Rack-aware placement policy

Rack-aware placement policy enforces different data replicas to be placed in different racks to guarantee the rack-level disaster tolerance for your production environment. A data center usually has a lot of racks, and each rack has many storage nodes. You can use `RackAwareEnsemblePlacementPolicy` to configure the rack information for each bookie.

#### Qualified rack size of bookies

When the available rack size of bookies can meet the requirements configured on a topic, the rack-aware placement policy can work well and you don’t need any extra configurations.

For example, the BookKeeper cluster has 4 racks and 13 bookie instances as shown in the following diagram. When a topic is configured with `EnsembleSize=3, WriteQuorum=3, AckQuorum=2`, the BookKeeper client chooses one bookie instance from three different racks to write data to, such as Bookie2, Bookie8, and Bookie12.


![Rack-aware placement policy](/assets/rack-aware-placement-policy-1.svg)

#### Enforced minimum rack size of bookies

When the available rack size of bookies cannot meet the requirements configured on a topic, the strategy that the BookKeeper client chooses bookies to recover old ledgers and create new ledgers depends on whether the enforced minimum rack size of bookies is configured.

In this case, if you want to make the rack-aware placement policy work as usual, you need to configure an enforced minimum rack size of bookies (`MinNumRacksPerWriteQuorum`).

For example, you have the same BookKeeper cluster with the same topic requirements `EnsembleSize=3, WriteQuorum=3, AckQuorum=2` as shown in the above diagram. When all the bookie instances in Rack3 and Rack4 fail, you only have 2 available racks and there are the following three possibilities.

* If you have configured `EnforceMinNumRacksPerWriteQuorum=true` and `MinNumRacksPerWriteQuorum=3`, the BookKeeper client fails to choose bookies, which means new ledgers cannot be created and old ledgers cannot be recovered. Because the requirement of `MinNumRacksPerWriteQuorum=3` cannot be fulfilled.

* If you have configured `EnforceMinNumRacksPerWriteQuorum=true` and `MinNumRacksPerWriteQuorum=2`, the BookKeeper client chooses one bookie from Rack1 and Rack2 to recover old ledgers, such as bookie1 and bookie5, to place 2 replicas for Bookie8 and Bookie12. For new ledger creation, it chooses one bookie from Rack1 and Rack2, such as Bookie4 and Bookie7, and a random bookie from either Rack1 or Rack2 to place the last replica.

![Rack-aware placement policy with an enforced minimum rack size of bookies](/assets/rack-aware-placement-policy-2.svg)

* If you have configured `EnforceMinNumRacksPerWriteQuorum=false`, the BookKeeper client tries its best effort to apply the placement policy depending on the available number of racks and bookies. It may still work as the above diagram or the following diagram. 

![Rack-aware placement policy without an enforced minimum rack size of bookies](/assets/rack-aware-placement-policy-3.svg)

### Region-aware placement policy

Region-aware placement policy enforces different data replicas to be placed in different regions and racks to guarantee region-level disaster tolerance. To achieve datacenter level disaster tolerance, you need to write data replicas into different data centers. You can use `RegionAwareEnsemblePlacementPolicy` to configure region and rack information for each bookie node to ensure region-level disaster tolerance.

For example, the BookKeeper cluster has 4 regions, and each region has several racks with their bookie instances, as shown in the following diagram. If a topic is configured with `EnsembleSize=3, WriteQuorum=3, and AckQuorum=2`, the BookKeeper client chooses three different regions, such as Region A, Region C and Region D. For each region, it chooses one bookie on a single rack, such as Bookie5 on Rack2, Bookie17 on Rack6, and Bookie21 on Rack8.

![Region-aware placement policy](/assets/region-aware-placement-policy-1.svg)

When two regions fail, such as Region B and Region C, as shown in the following diagram, the BookKeeper client chooses one bookie from Region A or Region D to replace the failed Bookie17 for recovering old ledgers. And it also chooses Region A and Region D to write replicas for creating new ledgers. In Region A, it falls back to rack-aware placement policy and chooses one bookie from Rack1 and Rack2, such as Bookie4 and Bookie7. For Region D, it has to choose one bookie from Rack8, such as Bookie22.

![Region-aware placement policy for disaster tolerance](/assets/region-aware-placement-policy-2.svg)

## Enable bookie data placement policy

By default, the rack-aware placement policy is enabled on both broker and bookie sides. If you want to switch to the region-aware placement policy, you need to enable the region-aware placement policy on both broker and bookie sides.

### Enable region-aware placement policy on broker

Configure the following field in the `conf/broker.conf` file.

```properties
bookkeeperClientRegionawarePolicyEnabled=true
```

To enforce the minimum rack size of bookies, configure the following fields:

```properties
bookkeeperClientEnforceMinNumRacksPerWriteQuorum=true
bookkeeperClientMinNumRacksPerWriteQuorum=2
```

To balance the ledger disk usage of different bookies, you can enable the disk weight placement by configuring the following field:

```properties
bookkeeperDiskWeightBasedPlacementEnabled=true
```
### Enable region-aware placement policy on the auto-recovery instances (pods)

Configure the following fields in the `conf/bookkeeper.conf` file.

```properties
ensemblePlacementPolicy=org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy
reppDnsResolverClass=org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping
```

To enforce the minimum rack size of bookies, configure the following fields:

```properties
enforceMinNumRacksPerWriteQuorum=true
minNumRacksPerWriteQuorum=2
```

To balance the ledger disk usage of different bookies, you can enable the disk weight placement by configuring the following field:

```properties
diskWeightBasedPlacementEnabled=true
```

## Configure data placement policy on bookie instances

To configure a data placement policy on bookie instances, you can use one of the following methods.

````mdx-code-block
<Tabs
  defaultValue="Pulsar-admin CLI"
  values={[{"label":"Pulsar-admin CLI","value":"Pulsar-admin CLI"},{"label":"REST API","value":"REST API"}]}>

<TabItem value="Pulsar-admin CLI">


Specify the rack name to represent which region or rack this bookie belongs to.

```bash
bin/pulsar-admin bookies set-bookie-rack
The following options are required: [-b | --bookie], [-r | --rack]

Then we need to update the rack placement information for a specific bookie in the cluster. Note that the bookie address format is `address:port`.
Usage: set-bookie-rack [options]
  Options:
  * -b, --bookie
      Bookie address (format: `address:port`)
    -g, --group
      Bookie group name
      Default: default
    --hostname
      Bookie host name
  * -r, --rack
      Bookie rack name
```

:::tip

In addition, you can also group bookies across racks or regions to serve broker-level isolation by specifying a group name for each bookie and assigning the group name to a specific namespace. See [configure bookie affinity groups](#configure-bookie-affinity-groups) for more details.

:::

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/bookies/racks-info/:bookie|operation/updateBookieRackInfo?version=@pulsar:version_number@}

</TabItem>

</Tabs>
````


#### Example of configuring rack-aware placement policy

The following is an example of how to configure bookie instances with their rack properties.

```bash
bin/pulsar-admin bookies set-bookie-rack --bookie bookie1:3181 --hostname bookie1.pulsar.com:3181 --group group1 --rack rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie2:3181 --hostname bookie2.pulsar.com:3181 --group group1 --rack rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie3:3181 --hostname bookie3.pulsar.com:3181 --group group1 --rack rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie4:3181 --hostname bookie4.pulsar.com:3181 --group group1 --rack rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie5:3181 --hostname bookie5.pulsar.com:3181 --group group1 --rack rack2
...
```

#### Example of configuring region-aware placement policy

The following is an example of how to configure bookie instances with their region/rack properties.

```bash
bin/pulsar-admin bookies set-bookie-rack --bookie bookie1:3181 --hostname bookie1.pulsar.com:3181 --group group1 --rack RegionA/rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie2:3181 --hostname bookie2.pulsar.com:3181 --group group1 --rack RegionA/rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie3:3181 --hostname bookie3.pulsar.com:3181 --group group1 --rack RegionA/rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie4:3181 --hostname bookie4.pulsar.com:3181 --group group1 --rack RegionA/rack1
bin/pulsar-admin bookies set-bookie-rack --bookie bookie5:3181 --hostname bookie5.pulsar.com:3181 --group group1 --rack RegionA/rack2
bin/pulsar-admin bookies set-bookie-rack --bookie bookie6:3181 --hostname bookie6.pulsar.com:3181 --group group1 --rack RegionA/rack2
bin/pulsar-admin bookies set-bookie-rack --bookie bookie7:3181 --hostname bookie7.pulsar.com:3181 --group group1 --rack RegionA/rack2
bin/pulsar-admin bookies set-bookie-rack --bookie bookie8:3181 --hostname bookie8.pulsar.com:3181 --group group1 --rack RegionB/rack3
...
```

## Configure bookie affinity groups

The data of a namespace can be isolated into user-defined groups of bookies, as known as bookie affinity groups, which guarantee all the data that belongs to the namespace is stored in desired bookies.

**Prerequisites:** Before configuring bookie affinity groups, you need to group bookies first. See [configure data placement policy on bookie instances](#configure-data-placement-policy-on-bookie-instances) for more details.

To configure bookie affinity groups, you can use one of the following methods.

````mdx-code-block
<Tabs
  defaultValue="Pulsar-admin CLI"
  values={[{"label":"Pulsar-admin CLI","value":"Pulsar-admin CLI"},{"label":"REST API","value":"REST API"},{"label":"Java admin API","value":"Java admin API"}]}>

<TabItem value="Pulsar-admin CLI">

```shell
pulsar-admin namespaces set-bookie-affinity-group options
```

For more information about the command `pulsar-admin namespaces set-bookie-affinity-group options`, see [Pulsar admin docs](/tools/pulsar-admin/).

**Example**

```shell
bin/pulsar-admin bookies set-bookie-rack \
--bookie 127.0.0.1:3181 \
--hostname 127.0.0.1:3181 \
--group group-bookie1 \
--rack rack1

bin/pulsar-admin namespaces set-bookie-affinity-group public/default \
--primary-group group-bookie1
```

:::note

- Do not set a bookie rack name to slash (`/`) or an empty string (`""`) if you use Pulsar earlier than 2.7.5, 2.8.3, and 2.9.2. If you use Pulsar 2.7.5, 2.8.3, 2.9.2 or later versions, it falls back to `/default-rack` or `/default-region/default-rack`.
- When `RackawareEnsemblePlacementPolicy` is enabled, the rack name is not allowed to contain one slash (`/`) except for the beginning and end of the rack name string. For example, a rack name like `/rack0` is allowed, but `/rack/0` is invalid.
- When `RegionAwareEnsemblePlacementPolicy` is enabled, the rack name can only contain one slash (`/`) except for the beginning and end of the rack name string. For example, rack name like `/region0/rack0` is allowed, but `/region0rack0` and `/region0/rack/0` are invalid.
For the bookie rack name restrictions, see [pulsar-admin bookies set-bookie-rack](/tools/pulsar-admin/).

:::

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/persistence/bookieAffinity|operation/setBookieAffinityGroup?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java admin API">

For how to set bookie affinity group for a namespace using Java admin API, see [code](https://github.com/apache/pulsar/blob/master/pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/NamespacesImpl.java#L1164).

</TabItem>

</Tabs>
````
