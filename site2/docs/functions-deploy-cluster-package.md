---
id: functions-deploy-cluster-package
title: Enable package management service
sidebar_label: "Enable package management service"
---

[Package management service](admin-api-packages.md) enables both version management and simplified upgrade/rollback processes for functions, sinks, and sources. When using the same function, sink, and source in different namespaces, you can upload them to a common package management system.

With the package management service enabled, you can [upload your function package](/tools/pulsar-admin/) to the service and get the package URL. Thus you can create the function by setting `--jar`, `--py`, or `--go` to the package URL. 

By default, the package management service is disabled. To enable it in your cluster, set the properties in the `conf/broker.conf` file as follows.

```conf

enablePackagesManagement=true
packagesManagementStorageProvider=org.apache.pulsar.packages.management.storage.bookkeeper.BookKeeperPackagesStorageProvider
packagesReplicas=1
packagesManagementLedgerRootPath=/ledgers

```

:::tip

To ensure high availability in a production deployment (a cluster with multiple brokers), set `packagesReplicas` to equal the number of bookies. The default value `1` is only for one-node cluster deployment. 

:::
