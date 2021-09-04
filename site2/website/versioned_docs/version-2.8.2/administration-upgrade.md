---
id: version-2.8.2-administration-upgrade
title: Upgrade Guide
sidebar_label: Upgrade
original_id: administration-upgrade
---

## Upgrade guidelines

Apache Pulsar is comprised of multiple components, ZooKeeper, bookies, and brokers. These components are either stateful or stateless. You do not have to upgrade ZooKeeper nodes unless you have special requirement. While you upgrade, you need to pay attention to bookies (stateful), brokers and proxies (stateless).

The following are some guidelines on upgrading a Pulsar cluster. Read the guidelines before upgrading.

- Backup all your configuration files before upgrading.
- Read guide entirely, make a plan, and then execute the plan. When you make upgrade plan, you need to take your specific requirements and environment into consideration.   
- Pay attention to the upgrading order of components. In general, you do not need to upgrade your ZooKeeper or configuration store cluster (the global ZooKeeper cluster). You need to upgrade bookies first, and then upgrade brokers, proxies, and your clients. 
- If `autorecovery` is enabled, you need to disable `autorecovery` in the upgrade process, and re-enable it after completing the process.
- Read the release notes carefully for each release. Release notes contain features, configuration changes that might impact your upgrade.
- Upgrade a small subset of nodes of each type to canary test the new version before upgrading all nodes of that type in the cluster. When you have upgraded the canary nodes, run for a while to ensure that they work correctly.
- Upgrade one data center to verify new version before upgrading all data centers if your cluster runs in multi-cluster replicated mode.

> Note: Currently, Apache Pulsar is compatible between versions. 

## Upgrade sequence

To upgrade an Apache Pulsar cluster, follow the upgrade sequence.

1. Upgrade ZooKeeper (optional)  
- Canary test: test an upgraded version in one or a small set of ZooKeeper nodes.  
- Rolling upgrade: rollout the upgraded version to all ZooKeeper servers incrementally, one at a time. Monitor your dashboard during the whole rolling upgrade process.
2. Upgrade bookies  
- Canary test: test an upgraded version in one or a small set of bookies.
- Rolling upgrade:  
    - a. Disable `autorecovery` with the following command.
       ```shell
       bin/bookkeeper shell autorecovery -disable
       ```  
    - b. Rollout the upgraded version to all bookies in the cluster after you determine that a version is safe after canary.  
    - c. After you upgrade all bookies, re-enable `autorecovery` with the following command.
       ```shell
       bin/bookkeeper shell autorecovery -enable
       ```
3. Upgrade brokers
- Canary test: test an upgraded version in one or a small set of brokers.
- Rolling upgrade: rollout the upgraded version to all brokers in the cluster after you determine that a version is safe after canary.
4. Upgrade proxies
- Canary test: test an upgraded version in one or a small set of proxies.
- Rolling upgrade: rollout the upgraded version to all proxies in the cluster after you determine that a version is safe after canary.

## Upgrade ZooKeeper (optional)
While you upgrade ZooKeeper servers, you can do canary test first, and then upgrade all ZooKeeper servers in the cluster.

### Canary test

You can test an upgraded version in one of ZooKeeper servers before upgrading all ZooKeeper servers in your cluster.

To upgrade ZooKeeper server to a new version, complete the following steps:

1. Stop a ZooKeeper server.
2. Upgrade the binary and configuration files.
3. Start the ZooKeeper server with the new binary files.
4. Use `pulsar zookeeper-shell` to connect to the newly upgraded ZooKeeper server and run a few commands to verify if it works as expected.
5. Run the ZooKeeper server for a few days, observe and make sure the ZooKeeper cluster runs well.

#### Canary rollback

If issues occur during canary test, you can shut down the problematic ZooKeeper node, revert the binary and configuration, and restart the ZooKeeper with the reverted binary.

### Upgrade all ZooKeeper servers

After canary test to upgrade one ZooKeeper in your cluster, you can upgrade all ZooKeeper servers in your cluster. 

You can upgrade all ZooKeeper servers one by one by following steps in canary test.

## Upgrade bookies

While you upgrade bookies, you can do canary test first, and then upgrade all bookies in the cluster.
For more details, you can read Apache BookKeeper [Upgrade guide](http://bookkeeper.apache.org/docs/latest/admin/upgrade).

### Canary test

You can test an upgraded version in one or a small set of bookies before upgrading all bookies in your cluster.

To upgrade bookie to a new version, complete the following steps:

1. Stop a bookie.
2. Upgrade the binary and configuration files.
3. Start the bookie in `ReadOnly` mode to verify if the bookie of this new version runs well for read workload.
   ```shell
   bin/pulsar bookie --readOnly
   ```
4. When the bookie runs successfully in `ReadOnly` mode, stop the bookie and restart it in `Write/Read` mode.
   ```shell
   bin/pulsar bookie
   ```
5. Observe and make sure the cluster serves both write and read traffic.

#### Canary rollback

If issues occur during the canary test, you can shut down the problematic bookie node. Other bookies in the cluster replaces this problematic bookie node with autorecovery. 

### Upgrade all bookies

After canary test to upgrade some bookies in your cluster, you can upgrade all bookies in your cluster. 

Before upgrading, you have to decide whether to upgrade the whole cluster at once, including downtime and rolling upgrade scenarios.

In a rolling upgrade scenario, upgrade one bookie at a time. In a downtime upgrade scenario, shut down the entire cluster, upgrade each bookie, and then start the cluster.

While you upgrade in both scenarios, the procedure is the same for each bookie.

1. Stop the bookie. 
2. Upgrade the software (either new binary or new configuration files).
2. Start the bookie.

> **Advanced operations**   
> When you upgrade a large BookKeeper cluster in a rolling upgrade scenario, upgrading one bookie at a time is slow. If you configure rack-aware or region-aware placement policy, you can upgrade bookies rack by rack or region by region, which speeds up the whole upgrade process.

## Upgrade brokers and proxies

The upgrade procedure for brokers and proxies is the same. Brokers and proxies are `stateless`, so upgrading the two services is easy.

### Canary test

You can test an upgraded version in one or a small set of nodes before upgrading all nodes in your cluster.

To upgrade to a new version, complete the following steps:

1. Stop a broker (or proxy).
2. Upgrade the binary and configuration file.
3. Start a broker (or proxy).

#### Canary rollback

If issues occur during canary test, you can shut down the problematic broker (or proxy) node. Revert to the old version and restart the broker (or proxy).

### Upgrade all brokers or proxies

After canary test to upgrade some brokers or proxies in your cluster, you can upgrade all brokers or proxies in your cluster. 

Before upgrading, you have to decide whether to upgrade the whole cluster at once, including downtime and rolling upgrade scenarios.

In a rolling upgrade scenario, you can upgrade one broker or one proxy at a time if the size of the cluster is small. If your cluster is large, you can upgrade brokers or proxies in batches. When you upgrade a batch of brokers or proxies, make sure the remaining brokers and proxies in the cluster have enough capacity to handle the traffic during upgrade.

In a downtime upgrade scenario, shut down the entire cluster, upgrade each broker or proxy, and then start the cluster.

While you upgrade in both scenarios, the procedure is the same for each broker or proxy.

1. Stop the broker or proxy. 
2. Upgrade the software (either new binary or new configuration files).
3. Start the broker or proxy.
