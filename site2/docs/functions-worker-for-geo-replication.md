---
id: functions-worker-for-geo-replication
title: Configure function workers for geo-replicated clusters
sidebar_label: "Configure function workers for geo-replicated clusters"
---

When running multiple clusters tied together with [geo replication](concepts-replication.md), you need to use a different function namespace for each cluster. Otherwise, all functions share one namespace and potentially schedule assignments across clusters.

For example, if you have two clusters: `east-1` and `west-1`, you can configure the function workers for `east-1` and `west-1` respectively in the `conf/functions_worker.yml` file. This ensures the two different function workers use distinct sets of topics for their internal coordination.

```yaml

pulsarFunctionsCluster: east-1
pulsarFunctionsNamespace: public/functions-east-1

```

```yaml

pulsarFunctionsCluster: west-1
pulsarFunctionsNamespace: public/functions-west-1

```
