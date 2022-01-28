---
id: version-2.4.2-admin-api-functions
title: Managing functions
sidebar_label: Functions
original_id: admin-api-functions
---

**Pulsar Functions** are lightweight compute processes that

* consume messages from one or more Pulsar topics
* apply a user-supplied processing logic to each message
* publish the results of the computation to another topic

Functions can be managed via the following methods.

Method | Description
---|---
**Admin CLI** | The [`functions`](reference-pulsar-admin.md#functions) command of the [`pulsar-admin`](reference-pulsar-admin.md) tool.
**REST API** |The `/admin/v3/functions` endpoint of the admin {@inject: rest:REST:/} API.
**Java Admin API**| The `functions` method of the {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object in the [Java API](client-libraries-java.md).

## Function resources

You can perform the following operations on functions.

### Create a function

You can create a Pulsar function in cluster mode (deploy it on a Pulsar cluster) using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`create`](reference-pulsar-admin.md#functions-create) subcommand. 

**Example**

```shell
$ pulsar-admin functions create \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --inputs test-input-topic \
  --output persistent://public/default/test-output-topic \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --jar /examples/api-examples.jar
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName?version=[[pulsar:version_number]]}

#### Java Admin API

```java
FunctionConfig functionConfig = new FunctionConfig();
functionConfig.setTenant(tenant);
functionConfig.setNamespace(namespace);
functionConfig.setName(functionName);
functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
functionConfig.setParallelism(1);
functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
functionConfig.setTopicsPattern(sourceTopicPattern);
functionConfig.setSubName(subscriptionName);
functionConfig.setAutoAck(true);
functionConfig.setOutput(sinkTopic);
admin.functions().createFunction(functionConfig, fileName);
```

### Update a function

You can update a Pulsar function that has been deployed to a Pulsar cluster using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`update`](reference-pulsar-admin.md#functions-update) subcommand. 

**Example**

```shell
$ pulsar-admin functions update \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --output persistent://public/default/update-output-topic \
  # other options
```

#### REST Admin API

{@inject: endpoint|PUT|/admin/v3/functions/:tenant/:namespace/functionName?version=[[pulsar:version_number]]}

#### Java Admin API

```java
FunctionConfig functionConfig = new FunctionConfig();
functionConfig.setTenant(tenant);
functionConfig.setNamespace(namespace);
functionConfig.setName(functionName);
functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
functionConfig.setParallelism(1);
functionConfig.setClassName("org.apache.pulsar.functions.api.examples.ExclamationFunction");
UpdateOptions updateOptions = new UpdateOptions();
updateOptions.setUpdateAuthData(updateAuthData);
admin.functions().updateFunction(functionConfig, userCodeFile, updateOptions);
```

### Start an instance of a function

You can start a stopped function instance with `instance-id` using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`start`](reference-pulsar-admin.md#functions-start) subcommand. 

```shell
$ pulsar-admin functions start \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/:instanceId/start?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().startFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Start all instances of a function

You can start all stopped function instances using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`start`](reference-pulsar-admin.md#functions-start) subcommand. 

**Example**

```shell
$ pulsar-admin functions start \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/start?version=[[pulsar:version_number]]}

#### Java

```java
admin.functions().startFunction(tenant, namespace, functionName);
```

### Stop an instance of a function

You can stop a function instance with `instance-id` using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`stop`](reference-pulsar-admin.md#functions-stop) subcommand. 

**Example**

```shell
$ pulsar-admin functions stop \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/:instanceId/stop?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().stopFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Stop all instances of a function

You can stop all function instances using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`stop`](reference-pulsar-admin.md#functions-stop) subcommand. 

**Example**

```shell
$ pulsar-admin functions stop \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/stop?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().stopFunction(tenant, namespace, functionName);
```

### Restart an instance of a function

Restart a function instance with `instance-id` using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`restart`](reference-pulsar-admin.md#functions-restart) subcommand. 

**Example**

```shell
$ pulsar-admin functions restart \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/:instanceId/restart?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().restartFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Restart all instances of a function

You can restart all function instances using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`restart`](reference-pulsar-admin.md#functions-restart) subcommand. 

**Example**

```shell
$ pulsar-admin functions restart \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/restart?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().restartFunction(tenant, namespace, functionName);
```

### List all functions

You can list all Pulsar functions running under a specific tenant and namespace using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`list`](reference-pulsar-admin.md#functions-list) subcommand.

**Example**

```shell
$ pulsar-admin functions list \
  --tenant public \
  --namespace default
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunctions(tenant, namespace);
```

### Delete a function

You can delete a Pulsar function that is running on a Pulsar cluster using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`delete`](reference-pulsar-admin.md#functions-delete) subcommand. 

**Example**

```shell
$ pulsar-admin functions delete \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|DELETE|/admin/v3/functions/:tenant/:namespace/functionName?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().deleteFunction(tenant, namespace, functionName);
```

### Get info about a function

You can get information about a Pulsar function currently running in cluster mode using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`get`](reference-pulsar-admin.md#functions-get) subcommand. 

**Example**

```shell
$ pulsar-admin functions get \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunction(tenant, namespace, functionName);
```

### Get status of an instance of a function

You can get the current status of a Pulsar function instance with `instance-id` using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`status`](reference-pulsar-admin.md#functions-status) subcommand. 

**Example**

```shell
$ pulsar-admin functions status \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName/:instanceId/status?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunctionStatus(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Get status of all instances of a function

You can get the current status of a Pulsar function instance using Admin CLI, REST API or Java Admin API.

#### Admin CLI

Use the [`status`](reference-pulsar-admin.md#functions-status) subcommand. 

**Example**

```shell
$ pulsar-admin functions status \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName/status?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunctionStatus(tenant, namespace, functionName);
```

### Get stats of an instance of a function

You can get the current stats of a Pulsar Function instance with `instance-id` using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`stats`](reference-pulsar-admin.md#functions-stats) subcommand. 

**Example**

```shell
$ pulsar-admin functions stats \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName/:instanceId/stats?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunctionStats(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Get stats of all instances of a function

You can get the current stats of a Pulsar function using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`stats`](reference-pulsar-admin.md#functions-stats) subcommand. 

**Example**

```shell
$ pulsar-admin functions stats \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName/stats?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().getFunctionStats(tenant, namespace, functionName);
```

### Trigger a function

You can trigger a specified Pulsar function with a supplied value using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`trigger`](reference-pulsar-admin.md#functions-trigger) subcommand. 

**Example**

```shell
$ pulsar-admin functions trigger \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --topic (the name of input topic) \
  --trigger-value \"hello pulsar\"
  # or --trigger-file (the path of trigger file)
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/trigger?version=[[pulsar:version_number]]}

#### Java Admin API

```java
admin.functions().triggerFunction(tenant, namespace, functionName, topic, triggerValue, triggerFile);
```

### Put state associated with a function

You can put the state associated with a Pulsar function using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`putstate`](reference-pulsar-admin.md#functions-putstate) subcommand. 

**Example**

```shell
$ pulsar-admin functions putstate \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --state "{\"key\":\"pulsar\", \"stringValue\":\"hello pulsar\"}" 
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/:tenant/:namespace/functionName/state/:key?version=[[pulsar:version_number]]}

#### Java Admin API

```java
TypeReference<FunctionState> typeRef = new TypeReference<FunctionState>() {};
FunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue(state, typeRef);
admin.functions().putFunctionState(tenant, namespace, functionName, stateRepr);
```

### Fetch state associated with a function

You can fetch the current state associated with a Pulsar function using Admin CLI, REST API or Java admin API.

#### Admin CLI

Use the [`querystate`](reference-pulsar-admin.md#functions-querystate) subcommand. 

**Example**

```shell
$ pulsar-admin functions querystate \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --key (the key of state) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/:tenant/:namespace/functionName/state/:key?version=[[pulsar:version_number]]}

#### Java Admin CLI

```java
admin.functions().getFunctionState(tenant, namespace, functionName, key);
```
