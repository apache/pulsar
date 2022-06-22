---
id: functions-worker-stateful
title: Enable stateful functions
sidebar_label: "Enable stateful functions"
---

:::note

When the stateful APIs of Pulsar Functions are required – for example, `putState()` and `queryState()` related interfaces – you need to enable the stateful function feature in function workers.

:::

1. Enable the `streamStorage` service in BookKeeper.
   Currently, the service uses the NAR package, so you need to set the configuration in the `conf/bookkeeper.conf` file.

   ```text
   
   ##################################################################
   ##################################################################
   # Settings below are used by stream/table service
   ##################################################################
   ##################################################################
   
   ### Grpc Server ###

   # the grpc server port to listen on. default is 4181
   storageserver.grpc.port=4181

   ### Dlog Settings for table service ###

   #### Replication Settings
   dlog.bkcEnsembleSize=3
   dlog.bkcWriteQuorumSize=2
   dlog.bkcAckQuorumSize=2

   ### Storage ###

   # local storage directories for storing table ranges data (e.g. rocksdb sst files)
   storage.range.store.dirs=data/bookkeeper/ranges

   # whether the storage server capable of serving readonly tables. default is false.
   storage.serve.readonly.tables=false

   # the cluster controller schedule interval, in milliseconds. default is 30 seconds.
   storage.cluster.controller.schedule.interval.ms=30000

   ```

2. After starting the bookie, use the following methods to check whether the `streamStorage` service has been started successfully.

   * Input:

      ```shell

      telnet localhost 4181

      ```

   * Output:

       ```text

      Trying 127.0.0.1...
      Connected to localhost.
      Escape character is '^]'.

      ```

3. Configure `stateStorageServiceUrl` in the `conf/functions_worker.yml` file. 
   `bk-service-url` is the service URL pointing to the BookKeeper table service.

   ```shell

   stateStorageServiceUrl: bk://<bk-service-url>:4181

   ```
