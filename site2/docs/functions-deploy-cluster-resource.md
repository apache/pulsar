---
id: functions-deploy-cluster-resource
title: Allocate resources to function instance
sidebar_label: "Allocate resources to function instance"
---

When running functions in cluster mode, you can specify the resources that can be allocated to each function instance.

The following table outlines the resources that can be allocated to function instances.

| Resource   | Specified as        | Supported runtime |
|------------|---------------------|-------------------|
| CPU        | The number of cores | Kubernetes        |
| RAM        | The number of bytes | Kubernetes        |
| Disk space | The number of bytes | Kubernetes        |

For example, the following command allocates 8 cores, 8GB of RAM, and 10GB of disk space to a function.

```bash

bin/pulsar-admin functions create \
  --jar target/my-functions.jar \
  --classname org.example.functions.MyFunction \
  --cpu 8 \
  --ram 8589934592 \
  --disk 10737418240

```

:::note

The resources allocated to a given function are applied to each instance of the function. For example, if you apply 8GB of RAM to a function with a [parallelism](functions-deploy-cluster-parallelism.md) of 5, you are applying 40GB of RAM for the function in total. 

:::
