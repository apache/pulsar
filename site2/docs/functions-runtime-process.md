---
id: functions-runtime-process
title: Configure process runtime
sidebar_label: "Configure process runtime"
---

You can use the default configurations of process runtime in the `conf/functions_worker.yml` file. 

If you want to customize more parameters, refer to the following example.

```yaml

functionRuntimeFactoryClassName: org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory
functionRuntimeFactoryConfigs:
  # the directory for storing the function logs
  logDirectory:
  # change the jar location only when you put the java instance jar in a different location
  javaInstanceJarLocation:
  # change the python instance location only when you put the python instance jar in a different location
  pythonInstanceLocation:
  # change the extra dependencies location:
  extraFunctionDependenciesDir:

```
