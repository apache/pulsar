---
id: functions-worker-troubleshooting
title: Troubleshooting
sidebar_label: "Troubleshooting"
---

**Error message: Namespace missing local cluster name in clusters list**

```text

Failed to get partitioned topic metadata: org.apache.pulsar.client.api.PulsarClientException$BrokerMetadataException: Namespace missing local cluster name in clusters list: local_cluster=xyz ns=public/functions clusters=[standalone]

```

The error message displays when any of the following cases occurs:
- a broker is started with `functionsWorkerEnabled=true`, but `pulsarFunctionsCluster` in the `conf/functions_worker.yml` file is not set to the correct cluster.
- setting up a geo-replicated Pulsar cluster with `functionsWorkerEnabled=true`, while brokers in one cluster run well, brokers in the other cluster do not work well.

**Workaround**

If any of these cases happen, follow the instructions below to fix the problem.

1. Disable function workers by setting `functionsWorkerEnabled=false`, and restart brokers.

2. Get the current cluster list of the `public/functions` namespace.

   ```bash

   bin/pulsar-admin namespaces get-clusters public/functions

   ```

3. Check if the cluster is in the cluster list. If not, add it and update the list.

   ```bash

   bin/pulsar-admin namespaces set-clusters --clusters <existing-clusters>,<new-cluster> public/functions

   ```

4. After setting the cluster successfully, enable function workers by setting `functionsWorkerEnabled=true`. 

5. Set the correct cluster name for the `pulsarFunctionsCluster` parameter in the `conf/functions_worker.yml` file.

6. Restart brokers. 
