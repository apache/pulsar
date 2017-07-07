---
title: Pulsar cluster and broker administration
tags: [admin, cluster, broker, pulsar-admin, REST API, Java]
---

{% include explanations/admin-setup.md %}

## Managing clusters

{% include explanations/cluster-admin.md %}

In Pulsar, a {% popover cluster %} is a group of {% popover brokers %}, {% popover bookies %}, and other components. Pulsar {% popover instances %} are comprised of multiple Pulsar clusters that replicate to one another using {% popover geo-replication %}.

## Managing brokers

In Pulsar, a {% popover broker %} consists of two components: an HTTP server exposing a [REST interface](../../reference/RestApi) that powers both {% popover topic lookup %}


and a {% popover dispatcher %}

{% include explanations/broker-admin.md %}
