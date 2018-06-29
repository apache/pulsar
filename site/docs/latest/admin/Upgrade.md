---
title: Upgrade
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

> If you have questions about upgrades (or need help), please feel free to reach out to us by [mailing list]({{ site.baseurl }}/contact) or [Slack Channel]({{ site.baseurl }}/contact).

## Overview

Consider the below guidelines in preparation for upgrading.

- Always back up all your configuration files before upgrading.
- Read through the documentation and draft an upgrade plan that matches your specific requirements and environment before starting the upgrade process.
    Put differently, don't start working through the guide on a live cluster. Read guide entirely, make a plan, then execute the plan.
- Pay careful consideration to the order in which components are upgraded. In general, you need to upgrade bookies first, upgrade brokers next and then upgrade your clients.
- If autorecovery is running along with bookies, you need to pay attention to the upgrade sequence.
- Read the release notes carefully for each release. They contain not only information about noteworthy features, but also changes to configurations
    that may impact your upgrade.
- Always upgrade one or a small set of bookies/brokers to canary new version before upgraing all bookies/brokers in your cluster.
- Always upgrade one datacenter to canry new version before upgrading all datacenters if your cluster is running in multi-clusters replicated mode.

Following is the general guide to upgrade an Apache Pulsar cluster. If there is anything specials needed to be taken care, we will describe the details
for individual versions in "Upgrade Guides" section.

## Sequence

Follow the sequence below on upgrading an Apache Pulsar cluster

1. Upgrade Bookies
  1. Canary: canary an upgraded version in one or small set of bookies.
  2. Rolling Upgrade: rollout the upgraded version to all bookies in the cluster once you determined a version is safe after canary.
2. Upgrade Brokers
  1. Canary: canary an upgraded version in one or small set of brokers.
  2. Rolling Upgrade: rollout the upgraded version to all brokers in the cluster once you determined a version is safe after canary.
3. Upgrade Proxies
  1. Canary: canary an upgraded version in one or small set of proxies.
  2. Rolling Upgrade: rollout the upgraded version to all proxies in the cluster once you determined a version is safe after canary.

Typically you don't need to upgrade zookeeper unless there are strong reasons documented in "Upgrade Guides" section below.
But if you would like to keep your zookeeper cluster consistent with other components, follows the general upgrade guideline as others.

1. Canary: canary an upgraded version in one of the zookeeper servers.
2. Rolling Upgrade: rollout the upgraded version to all zookeeper servers incrementally, one at a time.

## Upgrade Bookies

> You can also read Apache BookKeeper [Upgrade Guide](http://bookkeeper.apache.org/docs/latest/admin/upgrade) for more details.

### Canary

It is wise to canary an upgraded version in one or small set of bookies before upgrading all bookies in your live cluster.

You can follow below steps on how to canary a upgraded version:

1. Stop a Bookie.
2. Upgrade the binary and configuration.
3. Start the Bookie in `ReadOnly` mode. This can be used to verify if the Bookie of this new version can run well for read workload.
   ```shell
   bin/pulsar bookie --readOnly
   ```
4. Once the Bookie is running at `ReadOnly` mode successfully for a while, restart the Bookie in `Write/Read` mode.
   ```shell
   bin/pulsar bookie
   ```
5. After step 4, the Bookie will serve both write and read traffic.

#### Rollback Canaries

If problems occur during canarying an upgraded version, you can simply take down the problematic Bookie node. The remain bookies in the old cluster
will repair this problematic bookie node by autorecovery. Nothing needs to be worried about.

### Upgrade Steps

Once you determined a version is safe to upgrade in a few bookies in your cluster, you can perform following steps to upgrade all bookies in your cluster.

1. Decide on performing a rolling upgrade or a downtime upgrade.
2. Upgrade all Bookies (more below)

#### Upgrade Bookies

In a rolling upgrade scenario, upgrade one Bookie at a time. In a downtime upgrade scenario, take the entire cluster down, upgrade each Bookie, then start the cluster.

For each Bookie:

1. Stop the bookie. 
2. Upgrade the software (either new binary or new configuration)
2. Start the bookie.

## Upgrade Brokers / Proxies

Brokers and Proxies are almost same. They are `stateless` comparing to Bookies. so it is very straightforward on upgrading these two services.

### Canary

Although they are `stateless` and easier to upgrade, it is still wise to canary an upgraded version in one or small set of brokers/proxies before upgrading
all brokers in your live cluster.

You can follow below steps on how to canary a upgraded version:

1. Stop a Broker (or Proxy).
2. Upgrade the binary and configuration.
3. Start a Broke (or Proxy)r

#### Rollback Canaries

If problems occur during canarying an upgraded version, you can simply take down the problematic Broker (or Proxy) node.
Revert back the old version and restart the broker (or proxy).

### Upgrade Steps

Once you determined a version is safe to upgrade in a few brokers/proxies in your cluster, you can perform following steps to upgrade all brokers/proxies in your cluster.

1. Decide on performing a rolling upgrade or a downtime upgrade.
2. Upgrade all Brokers/Proxies (more below)

#### Upgrade Brokers/Proxies

In a rolling upgrade scenario, it is recommended to upgrade one Broker or one Proxy at a time if the size of the cluster is small. If you are managing a large cluster,
you can plan upgrading brokers or proxies in batches. When doing so, make sure when you upgrading a batch of brokers or proxies, the remaining brokers and proxies in
the cluster have enough capacity to handle the traffic during upgrade.

In a downtime upgrade scenario, take the entire cluster down, upgrade each Broker/Proxy, then start the cluster.

For each Broker / Proxy:

1. Stop the broker or proxy. 
2. Upgrade the software (either new binary or new configuration)
2. Start the broker or proxy.

## Upgrade Guides

We describes the general upgrade method in Apache Pulsar as above. We will cover the details for individual versions below.
