---
id: functions-worker-corun
title: Run function workers with brokers
sidebar_label: "Run function workers with brokers"
---

The following diagram illustrates the deployment of function workers running along with brokers. 

![assets/functions-worker-corun.svg](/assets/function-workers-corun.svg)

:::note
 
The `Service URLs` in the illustration represent Pulsar service URLs that Pulsar client and Pulsar admin use to connect to a Pulsar cluster.

:::

To set up function workers to run with brokers, complete the following steps:
1. [Enable function workers](#enable-function-workers-to-run-with-brokers)
2. [Configure function workers](#configure-function-workers-to-run-with-brokers)
3. [Start function workers](#start-function-workers-to-run-with-brokers)


### Enable function workers to run with brokers

In the `conf/broker.conf` file, set `functionsWorkerEnabled` to `true`.

```conf

functionsWorkerEnabled=true

```

### Configure function workers to run with brokers

In the `run-with-brokers` mode, most settings of function workers are inherited from your broker configuration (for example, configuration store settings, authentication settings, and so on). You can customize other worker settings by configuring the `conf/functions_worker.yml` file based on your needs. 

:::tip

- To ensure high availability in a production deployment (a cluster with multiple brokers), set `numFunctionPackageReplicas` to equal the number of bookies. The default value `1` is only for one-node cluster deployment. 
- To initialize distributed log metadata in runtime (`initializedDlogMetadata` = `true`), ensure that it has been initialized by the `bin/pulsar initialize-cluster-metadata` command.

:::

When authentication is enabled on the BookKeeper cluster, you need to configure the following authentication settings for your function workers.
- `bookkeeperClientAuthenticationPlugin`: the authentication plugin name of BookKeeper client.
- `bookkeeperClientAuthenticationParametersName`: the authentication plugin parameters of BookKeeper client, including names and values.
- `bookkeeperClientAuthenticationParameters`: the authentication plugin parameters of BookKeeper client.

### Start function workers to run with brokers

Once function workers are configured properly, you can start the brokers (function workers are running with the brokers).

To verify whether each worker is running or not, you can use the following command.

```bash

curl <broker-ip>:8080/admin/v2/worker/cluster

```

If a list of active function workers is returned, it means they have been started successfully. The output is similar to the following.

```json

[{"workerId":"<worker-id>","workerHostname":"<worker-hostname>","port":8080}]

```