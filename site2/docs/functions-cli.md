---
id: functions-cli
title: Pulsar Functions CLI and YAML configs
sidebar_label: "CLI and YAML configs"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


## Pulsar admin CLI for Pulsar Functions

The Pulsar admin interface enables you to create and manage Pulsar Functions through CLI. For the latest and complete information, including commands, flags, and descriptions, refer to [Pulsar admin CLI](/tools/pulsar-admin/).


## YAML configurations for Pulsar Functions

You can configure a function by using a predefined YAML file. The following table outlines the required fields and arguments.

| Field Name           | Type                       | Related Command Argument   | Description|
|----------------------|----------------------------|----------------------------|------------|
| runtimeFlags         | String                     | N/A                        | Any flags that you want to pass to a runtime (for process & Kubernetes runtime only). |
| tenant               | String                     | `--tenant`                 | The tenant of a function.|
| namespace            | String                     | `--namespace`              | The namespace of a function.|
| name                 | String                     | `--name`                   | The name of a function.|
| className            | String                     | `--classname`              | The class name of a function. |
| inputs               | List`<String>`               | `-i`, `--inputs`           | The input topics of a function. Multiple topics can be specified as a comma-separated list. |
| customSerdeInputs    | Map`<String,String>`         | `--custom-serde-inputs`    | The mapping from input topics to SerDe class names. |
| topicsPattern        | String                     | `--topics-pattern`         | The topic pattern to consume from a list of topics under a namespace. <br />**Note:** `--input` and `--topic-pattern` are mutually exclusive. For Java functions, you need to add the SerDe class name for a pattern in `--custom-serde-inputs`. |
| customSchemaInputs   | Map`<String,String>`         | `--custom-schema-inputs`   | The mapping from input topics to schema properties. |
| customSchemaOutputs  | Map`<String,String>`         | `--custom-schema-outputs`  | The mapping from output topics to schema properties.|
| inputSpecs           | Map`<String,`[ConsumerConfig](#consumerconfig)`>` | `--input-specs` | The mapping from inputs to custom configurations.|
| output               | String                     | `-o`, `--output`           | The output topic of a function. If none is specified, no output is written.  |
| producerConfig       | [ProducerConfig](#produceconfig)  | `--producer-config` | The custom configurations for producers.  |
| outputSchemaType     | String                     | `-st`, `--schema-type`     | The built-in schema type or custom schema class name used for message outputs.   |
| outputSerdeClassName | String                     | `--output-serde-classname` | The SerDe class used for message outputs. |
| logTopic             | String                     | `--log-topic`              | The topic that the logs of a function are produced to.  |
| processingGuarantees | String | `--processing-guarantees` | The processing guarantees (delivery semantics) applied to a function. Available values: `ATLEAST_ONCE`, `ATMOST_ONCE`, `EFFECTIVELY_ONCE`.|
| retainOrdering       | Boolean                    | `--retain-ordering`	     | Whether functions consume and process messages in order or not. |
| retainKeyOrdering    | Boolean                    | `--retain-key-ordering`    | Whether functions consume and process messages in key order or not. |
| batchBuilder         | String           | `--batch-builder` | Use `producerConfig.batchBuilder` instead. <br />**Note**: `batchBuilder` will be deprecated in code soon. |
| forwardSourceMessageProperty | Boolean  | `--forward-source-message-property`  | Whether the properties of input messages are forwarded to output topics or not during processing. When the value is set to `false`, the forwarding is disabled. |
| userConfig           | Map`<String,Object>`         | `--user-config`         	 | User-defined config key/values. |
| secrets       | Map`<String,Object>` | `--secrets`	| The mapping from secretName to objects that encapsulate how the secret is fetched by the underlying secrets provider. |
| runtime       | String             | N/A          | The runtime of a function. Available values: `java`,`python`, `go`. |
| autoAck       | Boolean            | `--auto-ack` | Whether the framework acknowledges messages automatically or not. |
| maxMessageRetries    | Int      |	`--max-message-retries` | The number of retries to process a message before giving up. |
| deadLetterTopic      | String   | `--dead-letter-topic`   | The topic used for storing messages that are not processed successfully. |
| subName              | String   | `--subs-name`           | The name of Pulsar source subscription used for input-topic consumers if required.|
| parallelism          | Int      | `--parallelism`         | The parallelism factor of a function, that is, the number of function instances to run. |
| resources     | [Resources](#resources)	| N/A           | N/A |
| fqfn          | String          | `--fqfn`                | The Fully Qualified Function Name (FQFN) of a function. |
| windowConfig  | [WindowConfig](#windowconfig) | N/A       | N/A |		
| timeoutMs     | Long            | `--timeout-ms`          | The message timeout (in milliseconds). |
| jar           | String          | `--jar`                 | The path of the JAR file for a function (written in Java). It also supports URL paths that workers can download the package from, including HTTP, HTTPS, file (file protocol assuming that file already exists on worker host), and function (package URL from packages management service). |
| py            | String          | `--py`                  | The path of the main Python/Python wheel file for a function (written in Python). It also supports URL paths that workers can download the package from, including HTTP, HTTPS, file (file protocol assuming that file already exists on worker host), and function (package URL from packages management service).  |
| go            | String          | `--go`                  | Path to the main Go executable binary for the function (written in Go).  It also supports URL paths that workers can download the package from, including HTTP, HTTPS, file (file protocol assuming that file already exists on worker host), and function (package URL from packages management service). |
| cleanupSubscription  | Boolean   | N/A            | Whether the subscriptions that a function creates or uses should be deleted or not when the function is deleted. |
| customRuntimeOptions | String    | `--custom-runtime-options` | A string that encodes options to customize the runtime. |
| maxPendingAsyncRequests | Int    | `--max-message-retries`    | The max number of pending async requests per instance to avoid a large number of concurrent requests. |
| exposePulsarAdminClientEnabled | Boolean | N/A                | Whether the Pulsar admin client is exposed to function context or not. By default, it is disabled. |
| subscriptionPosition | String    | `--subs-position`          | The position of Pulsar source subscription used for consuming messages from a specified location. The default value is `Latest`.|


##### ConsumerConfig

The following table outlines the nested fields and related arguments under the `inputSpecs` field.

| Field Name           | Type                       | Related Command Argument   | Description|
|----------------------|----------------------------|----------------------------|------------|
| schemaType           | String                     | N/A                        | N/A |
| serdeClassName       | String                     | N/A                        | N/A |
| isRegexPattern       | Boolean                    | N/A                        | N/A |
| schemaProperties     | Map`<String,String>`         | N/A                        | N/A |
| consumerProperties   | Map`<String,String>`         | N/A                        | N/A |
| receiverQueueSize    | Int                        | N/A                        | N/A |
| cryptoConfig         | [CryptoConfig](#cryptoconfig)   | N/A                   |Refer to [code](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/common/functions/CryptoConfig.java). |
| poolMessages         | Boolean                    | N/A                        | N/A |

###### ProducerConfig

The following table outlines the nested fields and related arguments under the `producerConfig` field.

| Field Name                         | Type                          | Related Command Argument | Description                     |
|------------------------------------|-------------------------------|--------------------------|---------------------------------|
| maxPendingMessages                 | Int                           | N/A                      | The max size of a queue that holds messages pending to receive an acknowledgment from a broker. |
| maxPendingMessagesAcrossPartitions | Int                           | N/A                      | The number of `maxPendingMessages` across all partitions. |
| useThreadLocalProducers            | Boolean                       | N/A                      | N/A                             |
| cryptoConfig                       | [CryptoConfig](#cryptoconfig) | N/A                      | Refer to [code](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/common/functions/CryptoConfig.java).|
| batchBuilder                       | String                        | `--batch-builder`        | The type of batch construction method. Available values: `DEFAULT` and `KEY_BASED`. The default value is `DEFAULT`. |

###### Resources

The following table outlines the nested fields and related arguments under the `resources` field.

| Field Name | Type   | Related Command Argument | Description  |
|------------|--------|--------------------------|--------------|
| cpu        | double | `--cpu`                  | The CPU in cores that need to be allocated per function instance (for Kubernetes runtime only). |
| ram        | Long   | `--ram`                  | The RAM in bytes that need to be allocated per function instance (for process/Kubernetes runtime only).|
| disk       | Long   | `--disk`                 | The disk in bytes that need to be allocated per function instance (for Kubernetes runtime only). |

###### WindowConfig

The following table outlines the nested fields and related arguments under the `windowConfig` field.

| Field Name                    | Type   | Related Command Argument         | Description                                         |
|-------------------------------|--------|----------------------------------|-----------------------------------------------------|
| windowLengthCount             | Int    | `--window-length-count`          | The number of messages per window.                  |
| windowLengthDurationMs        | Long   | `--window-length-duration-ms`    | The time duration (in milliseconds) per window.     |
| slidingIntervalCount          | Int    | `--sliding-interval-count`       | The number of messages after which a window slides. |
| slidingIntervalDurationMs     | Long   | `--sliding-interval-duration-ms` | The time duration after which a window slides.      |
| lateDataTopic                 | String | N/A                              | N/A                                                 |
| maxLagMs                      | Long   | N/A                              | N/A                                                 |
| watermarkEmitIntervalMs       | Long   | N/A                              | N/A                                                 |
| timestampExtractorClassName   | String | N/A                              | N/A                                                 |
| actualWindowFunctionClassName | String | N/A                              | N/A                                                 |

###### CryptoConfig

The following table outlines the nested fields and related arguments under the `cryptoConfig` field.

| Field Name                  | Type                        | Related Command Argument | Description   |
|-----------------------------|-----------------------------|--------------------------|---------------|
| cryptoKeyReaderClassName    | String                      | N/A                      | Refer to [code](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/common/functions/CryptoConfig.java).   |
| cryptoKeyReaderConfig       | Map`<String, Object>`         | N/A                      | N/A   |
| encryptionKeys              | String[]                      | N/A                      | N/A   |
| producerCryptoFailureAction | ProducerCryptoFailureAction | N/A                      | N/A   |
| consumerCryptoFailureAction | ConsumerCryptoFailureAction | N/A                      | N/A   |


### Example

The following example shows how to configure a function using YAML or JSON.

````mdx-code-block
<Tabs 
  defaultValue="YAML"
  values={[{"label":"YAML","value":"YAML"},{"label":"JSON","value":"JSON"}]}>

<TabItem value="YAML">

```yaml
tenant: "public"
namespace: "default"
name: "config-file-function"
inputs: 
  - "persistent://public/default/config-file-function-input-1"
  - "persistent://public/default/config-file-function-input-2"
output: "persistent://public/default/config-file-function-output"
jar: "function.jar"
parallelism: 1
resources: 
  cpu: 8
  ram: 8589934592
autoAck: true
userConfig:
  foo: "bar"
```

</TabItem>
<TabItem value="JSON">

```json
{
  "tenant": "public",
  "namespace": "default",
  "name": "config-file-function",
  "inputs": [
    "persistent://public/default/config-file-function-input-1",
    "persistent://public/default/config-file-function-input-2"
  ],
  "output": "persistent://public/default/config-file-function-output",
  "jar": "function.jar",
  "parallelism": 1,
  "resources": {
    "cpu": 8,
    "ram": 8589934592
  },
  "autoAck": true,
  "userConfig": {
    "foo": "bar"
  }
}
```

</TabItem>
</Tabs>
````
