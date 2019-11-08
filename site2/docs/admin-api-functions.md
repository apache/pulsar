---
id: admin-api-functions
title: Managing Functions
sidebar_label: Functions
---

**Pulsar Functions** are lightweight compute processes that

* consume messages from one or more Pulsar topics,
* apply a user-supplied processing logic to each message,
* publish the results of the computation to another topic.

Functions can be managed via:

* The [`functions`](reference-pulsar-admin.md#functions) command of the [`pulsar-admin`](reference-pulsar-admin.md) tool
* The `/admin/v3/functions` endpoint of the admin {@inject: rest:REST:/} API
* The `functions` method of the {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object in the [Java API](client-libraries-java.md)

## Functions resources

### Create 

Create a Pulsar Function in cluster mode (deploy it on a Pulsar cluster) using the admin interface.

#### pulsar-admin

You can create a new function using the [`create`](reference-pulsar-admin.md#functions-create) subcommand. Here's an example:

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

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}

#### Java

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

### Update

Update a Pulsar Function that has been deployed to a Pulsar cluster using the admin interface.

#### pulsar-admin

You can update a Pulsar Function using the [`update`](reference-pulsar-admin.md#functions-update) subcommand. Here's an example:

```shell
$ pulsar-admin functions update \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --output persistent://public/default/update-output-topic \
  # other options
```

#### REST API

{@inject: endpoint|PUT|/admin/v3/functions/{tenant}/{namespace}/{functionName}

#### Java

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

### Start function instance

Starts a stopped function instance using the admin interface.

#### pulsar-admin

You can start a stopped function instance  using the [`start`](reference-pulsar-admin.md#functions-start) subcommand. Here's an example:

```shell
$ pulsar-admin functions start \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/{instanceId}/start

#### Java

```java
admin.functions().startFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Start all function instances

Starts all stopped function instances using the admin interface.

#### pulsar-admin

You can start all stopped function instances using the [`start`](reference-pulsar-admin.md#functions-start) subcommand. Here's an example:

```shell
$ pulsar-admin functions start \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/start

#### Java

```java
admin.functions().startFunction(tenant, namespace, functionName);
```

### Stop function instance

Stops function instance using the admin interface.

#### pulsar-admin

You can stops function instance using the [`stop`](reference-pulsar-admin.md#functions-stop) subcommand. Here's an example:

```shell
$ pulsar-admin functions stop \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/{instanceId}/stop

#### Java

```java
admin.functions().stopFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Stop all function instances

Stops all function instances using the admin interface.

#### pulsar-admin

You can stop all function instances using the [`stop`](reference-pulsar-admin.md#functions-stop) subcommand. Here's an example:

```shell
$ pulsar-admin functions stop \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/stop

#### Java

```java
admin.functions().stopFunction(tenant, namespace, functionName);
```

### Restart function instance

Restart function instance using the admin interface.

#### pulsar-admin

You can restart function instance using the [`restart`](reference-pulsar-admin.md#functions-restart) subcommand. Here's an example:

```shell
$ pulsar-admin functions restart \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/{instanceId}/restart

#### Java

```java
admin.functions().restartFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Restart all function instances

Restart all function instances using the admin interface.

#### pulsar-admin

You can restart all function instances using the [`restart`](reference-pulsar-admin.md#functions-restart) subcommand. Here's an example:

```shell
$ pulsar-admin functions restart \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/restart

#### Java

```java
admin.functions().restartFunction(tenant, namespace, functionName);
```

### List

List all Pulsar Functions running under a specific tenant and namespace.

#### pulsar-admin

You can list all Pulsar Functions using the [`list`](reference-pulsar-admin.md#functions-list) subcommand. Here's an example:

```shell
$ pulsar-admin functions list \
  --tenant public \
  --namespace default
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}

#### Java

```java
admin.functions().getFunctions(tenant, namespace);
```

### Delete

Delete a Pulsar Function that is running on a Pulsar cluster.

#### pulsar-admin

You can delete a Pulsar Function using the [`delete`](reference-pulsar-admin.md#functions-delete) subcommand. Here's an example:

```shell
$ pulsar-admin functions delete \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|DELETE|/admin/v3/functions/{tenant}/{namespace}/{functionName}

#### Java

```java
admin.functions().deleteFunction(tenant, namespace, functionName);
```

### Get

Get information about a Pulsar Function currently running in cluster mode.

#### pulsar-admin

You can get information about a Pulsar Function using the [`get`](reference-pulsar-admin.md#functions-get) subcommand. Here's an example:

```shell
$ pulsar-admin functions get \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}

#### Java

```java
admin.functions().getFunction(tenant, namespace, functionName);
```

### Displays the status of a Pulsar Function instance

Displays the current status of a Pulsar Function instance.

#### pulsar-admin

You can get the current status of a Pulsar Function instance using the [`status`](reference-pulsar-admin.md#functions-status) subcommand. Here's an example:

```shell
$ pulsar-admin functions status \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}/{instanceId}/status

#### Java

```java
admin.functions().getFunctionStatus(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Displays the status of a Pulsar Function

Displays the current status of a Pulsar Function.

#### pulsar-admin

You can get the current status of a Pulsar Function using the [`status`](reference-pulsar-admin.md#functions-status) subcommand. Here's an example:

```shell
$ pulsar-admin functions status \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}/status

#### Java

```java
admin.functions().getFunctionStatus(tenant, namespace, functionName);
```

### Displays the stats of a Pulsar Function instance

Displays the current stats of a Pulsar Function instance.

#### pulsar-admin

You can get the current stats of a Pulsar Function instance using the [`stats`](reference-pulsar-admin.md#functions-stats) subcommand. Here's an example:

```shell
$ pulsar-admin functions stats \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --instance-id 1
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}/{instanceId}/stats

#### Java

```java
admin.functions().getFunctionStats(tenant, namespace, functionName, Integer.parseInt(instanceId));
```

### Displays the stats of a Pulsar Function

Displays the current stats of a Pulsar Function.

#### pulsar-admin

You can get the current stats of a Pulsar Function using the [`stats`](reference-pulsar-admin.md#functions-stats) subcommand. Here's an example:

```shell
$ pulsar-admin functions stats \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}/stats

#### Java

```java
admin.functions().getFunctionStats(tenant, namespace, functionName);
```

### Trigger

Trigger the specified Pulsar Function with a supplied value.

#### pulsar-admin

You can trigger the Pulsar Function using the [`trigger`](reference-pulsar-admin.md#functions-trigger) subcommand. Here's an example:

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

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/trigger

#### Java

```java
admin.functions().triggerFunction(tenant, namespace, functionName, topic, triggerValue, triggerFile);
```

### Put state

Put the state associated with a Pulsar Function.

#### pulsar-admin

You can put the state associated with a Pulsar Function using the [`putstate`](reference-pulsar-admin.md#functions-putstate) subcommand. Here's an example:

```shell
$ pulsar-admin functions putstate \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --state "{\"key\":\"pulsar\", \"stringValue\":\"hello pulsar\"}" 
```

#### REST API

{@inject: endpoint|POST|/admin/v3/functions/{tenant}/{namespace}/{functionName}/state/{key}

#### Java

```java
TypeReference<FunctionState> typeRef = new TypeReference<FunctionState>() {};
FunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue(state, typeRef);
admin.functions().putFunctionState(tenant, namespace, functionName, stateRepr);
```

### Query state

Fetch the current state associated with a Pulsar Function.

#### pulsar-admin

You can fetch the current state associated with a Pulsar Function using the [`querystate`](reference-pulsar-admin.md#functions-querystate) subcommand. Here's an example:

```shell
$ pulsar-admin functions querystate \
  --tenant public \
  --namespace default \
  --name (the name of Pulsar Functions) \
  --key (the key of state) 
```

#### REST API

{@inject: endpoint|GET|/admin/v3/functions/{tenant}/{namespace}/{functionName}/state/{key}

#### Java

```java
admin.functions().getFunctionState(tenant, namespace, functionName, key);
```
