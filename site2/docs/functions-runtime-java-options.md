---
id: functions-runtime-java-options
title: Customize Java runtime options
sidebar_label: "Customize Java runtime options"
---

:::note

This setting **only** applies to process runtime and Kubernetes runtime.

:::

To pass additional arguments to the JVM command line for every process started by a function worker, you can configure the `additionalJavaRuntimeArguments` in the `conf/functions_worker.yml` file as follows.
- Add JMV flags, like `-XX:+ExitOnOutOfMemoryError`
- Pass custom system properties, like `-Dlog4j2.formatMsgNoLookups`

```yaml

additionalJavaRuntimeArguments: ['-XX:+ExitOnOutOfMemoryError','-Dfoo=bar']

```

