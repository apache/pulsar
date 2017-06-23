---
title: Pulsar cluster and broker administration
tags: [admin, cluster, broker, pulsar-admin, REST API, Java]
---

{% include explanations/admin-setup.md %}

In Pulsar, a cluster is a group of {% popover brokers %}, {% popover bookies %}, and other components. Pulsar {% popover instances %} are typically comprised of multiple Pulsar clusters that replicate to one another using {% popover geo-replication %}.

## Managing clusters

{% include explanations/cluster-admin.md %}

## Managing brokers

{% include explanations/broker-admin.md %}
