---
id: administration-upgrade
title: The Pulsar Upgrade Guide
sidebar_label: Upgrade
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Overview

Apache Pulsar is comprised of multiple components, zookeeper, bookies and brokers. These components are either
stateful or stateless. They will be taken care of differently. In general, the ZooKeeper nodes don't necessarily
need to be upgraded. Bookies are stateful so that's where you need to take the most care. Brokers and proxies
are stateless so less care is needed.

This page provides a guideline on upgrading a Pulsar cluster. Please consider the below guidelines in preparation
for upgrading.

- Always back up all your configuration files before upgrading.
- Read through the documentation and draft an upgrade plan that matches your specific requirements and environment
  before starting the upgrade process. In other words, don't start working through this guide on a live cluster.
  Read guide entirely, make a plan, and then execute the plan.
- Pay careful consideration to the order in which components are upgraded. In general, you don't need to upgrade
  your zookeeper or configuration store cluster (the global zookeeper cluster). For the remaining components, you
  need to upgrade bookies first, upgrade brokers and proxies next, and then upgrade your clients.
- If autorecovery is enabled, you need to disable `autorecovery` during the whole upgrade process,
  and re-enable it after the process is finished.
- Read the release notes carefully for each release. Not only do they contain information about noteworthy features,
  but also changes to configuration that may impact your upgrade.
- Always upgrade a small subset of nodes of each type to canary test the new version before upgrading all nodes of that type
  in the cluster. When you have upgraded the canary nodes, allow them to run for a while to ensure that they are working correctly.
- Always upgrade one datacenter to verify new version before upgrading all datacenters if your cluster is running in multi-clusters
  replicated mode.

Following is the detailed sequence to upgrade an Apache Pulsar cluster. If there is anything specials needed to be taken care,
we will describe the details for individual versions in "Upgrade Guides" section.

## Sequence

Follow the sequence below on upgrading an Apache Pulsar cluster

> Typically you don't need to upgrade zookeeper unless there are strong reasons documented in "Upgrade Guides" section below.

1. Upgrade ZooKeeper (optionally)
  1. Canary Test: test an upgraded version in one or small set of zookeeper nodes.
  2. Rolling Upgrade: rollout the upgraded version to all zookeeper servers incrementally, one at a time. Don't be too fast and always
     monitor your dashboard during the whole rolling upgrade process.
2. Upgrade Bookies
  1. Canary Test: test an upgraded version in one or small set of bookies.
  2. Rolling Upgrade:
    1. Disable *autorecovery* by running the following command:
       ```shell
       bin/bookkeeper shell autorecovery -disable
       ```
    2. rollout the upgraded version to all bookies in the cluster once you determined a version is safe after canary.
    3. After all the bookies are upgraded to run the new version, re-enable *autorecovery*:
       ```shell
       bin/bookkeeper shell autorecovery -enable
       ```
3. Upgrade Brokers
  1. Canary Test: test an upgraded version in one or small set of brokers.
  2. Rolling Upgrade: rollout the upgraded version to all brokers in the cluster once you determined a version is safe after canary.
4. Upgrade Proxies
  1. Canary Test: test an upgraded version in one or small set of proxies.
  2. Rolling Upgrade: rollout the upgraded version to all proxies in the cluster once you determined a version is safe after canary.

## Upgrade Bookies

> You can also read Apache BookKeeper [Upgrade Guide](http://bookkeeper.apache.org/docs/latest/admin/upgrade) for more details.

### Canary Test

It is wise to test an upgraded version in one or small set of bookies before upgrading all bookies in your live cluster.

You can follow below steps on how to canary a upgraded version:

1. Stop a Bookie.
2. Upgrade the binary and configuration.
3. Start the Bookie in `ReadOnly` mode. This can be used to verify if the Bookie of this new version can run well for read workload.
   ```shell
   bin/pulsar bookie --readOnly
   ```
4. Once the Bookie is running at `ReadOnly` mode successfully for a while, stop the bookie and restart it in `Write/Read` mode.
   ```shell
   bin/pulsar bookie
   ```
5. After step 4, the Bookie will serve both write and read traffic.

#### Canary Rollback

If problems occur during canarying an upgraded version, you can simply take down the problematic Bookie node. The remain bookies in the old cluster
will repair this problematic bookie node by autorecovery. Nothing needs to be worried about.

### Upgrade Steps

Once you determined a version is safe to upgrade in a few bookies in your cluster, you can perform following steps to upgrade all bookies in your cluster.

1. Decide whether you want to upgrade the whole cluster at once, implying downtime, or do a rolling upgrade.
2. Upgrade all Bookies (more below)

#### Upgrade Bookies

In a rolling upgrade scenario, upgrade one Bookie at a time. In a downtime upgrade scenario, take the entire cluster down, upgrade each Bookie, then start the cluster.

For each Bookie:

1. Stop the bookie. 
2. Upgrade the software (either new binary or new configuration)
2. Start the bookie.

> Advanced Operations: when rolling upgrade a large bookkeeper cluster, upgrading one bookie at a time is very slow.
> If you have configured rack-aware or region-aware placement policy, you can upgrade bookies rack by rack or region by region.
> It will speed up the whole upgrade process.

## Upgrade Brokers / Proxies

The upgrade proceedure for Brokers and Proxies are almost the same. They are `stateless` comparing to Bookies.
so it is very straightforward on upgrading these two services.

### Canary Test

Although they are `stateless` and easier to upgrade, it is still wise to verify a new version in one or small set of nodes before upgrading
all nodes in your live cluster.

You can follow below steps on how to canary a upgraded version:

1. Stop a Broker (or Proxy).
2. Upgrade the binary and configuration.
3. Start a Broke (or Proxy)r

#### Canary Rollback

If problems occur during canarying an upgraded version, you can simply take down the problematic Broker (or Proxy) node.
Revert back the old version and restart the broker (or proxy).

### Upgrade Steps

Once you determined a version is safe to upgrade in a few brokers/proxies in your cluster, you can perform following steps to upgrade all brokers/proxies in your cluster.

1. Decide whether you want to upgrade the whole cluster at once, implying downtime, or do a rolling upgrade.
2. Upgrade all Brokers/Proxies (more below)

#### Upgrade Brokers/Proxies

In a rolling upgrade scenario, it is recommended to upgrade one Broker or one Proxy at a time
if the size of the cluster is small. If you are managing a large cluster, you can plan upgrading brokers
or proxies in batches. When doing so, make sure when you upgrade a batch of brokers or proxies, the
remaining brokers and proxies in the cluster have enough capacity to handle the traffic during upgrade.

In a downtime upgrade scenario, take the entire cluster down, upgrade each Broker/Proxy, then start the cluster.

For each Broker / Proxy:

1. Stop the broker or proxy. 
2. Upgrade the software (either new binary or new configuration)
2. Start the broker or proxy.

## Upgrade Guides

We describes the general upgrade method in Apache Pulsar as above.
We cover the details for individual versions required special attention below.

Currently Pulsar is compatible between versions. Nothing is required special attention.
