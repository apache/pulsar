---
id: functions-runtime-thread
title: Configure thread runtime
sidebar_label: "Configure thread runtime"
---

You can use the default configurations of thread runtime in the `conf/functions_worker.yml` file. If you want to customize parameters, such as thread group name, refer to the following example.

```yaml

functionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory
functionRuntimeFactoryConfigs:
  threadGroupName: "Your Function Container Group"

```

To set the client memory limit for thread runtime, you can configure `pulsarClientMemoryLimit`.

```yaml

functionRuntimeFactoryConfigs:
#  pulsarClientMemoryLimit
# # the max memory in bytes the pulsar client can use
#   absoluteValue:
# # the max memory the pulsar client can use as a percentage of max direct memory set for JVM
#   percentOfMaxDirectMemory:

```

:::note

If `absoluteValue` and `percentOfMaxDirectMemory` are both set, the smaller value is used.

:::