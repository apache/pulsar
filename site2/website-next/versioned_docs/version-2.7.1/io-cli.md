---
id: io-cli
title: Connector Admin CLI
sidebar_label: "CLI"
original_id: io-cli
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


The `pulsar-admin` tool helps you manage Pulsar connectors.
  
## `sources`

An interface for managing Pulsar IO sources (ingress data into Pulsar).

```bash

$ pulsar-admin sources subcommands

```

Subcommands are:

* `create`
  
* `update`
  
* `delete`
  
* `get`
  
* `status`
  
* `list`
  
* `stop`
  
* `start`
  
* `restart`
  
* `localrun`
  
* `available-sources`

* `reload`


### `create`

Submit a Pulsar IO source connector to run in a Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sources create options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the NAR archive for the source. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--classname` | The source's class name if `archive` is file-url-path (file://).
| `--cpu` | The CPU (in cores) that needs to be allocated per source instance (applicable only to Docker runtime).
| `--deserialization-classname` | The SerDe classname for the source.
| `--destination-topic-name` | The Pulsar topic to which data is sent.
| `--disk` | The disk (in bytes) that needs to be allocated per source instance (applicable only to Docker runtime).
|`--name` | The source's name.
| `--namespace` | The source's namespace.
| ` --parallelism` | The source's parallelism factor, that is, the number of source instances to run.
| `--processing-guarantees` | The processing guarantees (also named as delivery semantics) applied to the source. A source connector receives messages from external system and writes messages to a Pulsar topic. The `--processing-guarantees` is used to ensure the processing guarantees for writing messages to the Pulsar topic. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
| `--ram` | The RAM (in bytes) that needs to be allocated per source instance (applicable only to the process and Docker runtimes).
| `-st`, `--schema-type` | The schema type.<br /> Either a builtin schema (for example, AVRO and JSON) or custom schema class name to be used to encode messages emitted from source.
| `--source-config` | Source config key/values.
| `--source-config-file` | The path to a YAML config file specifying the source's configuration.
| `-t`, `--source-type` | The source's connector provider.
| `--tenant` | The source's tenant.
|`--producer-config`| The custom producer configuration (as a JSON string).

### `update`

Update a already submitted Pulsar IO source connector.

#### Usage

```bash

$ pulsar-admin sources update options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the NAR archive for the source. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--classname` | The source's class name if `archive` is file-url-path (file://).
| `--cpu` | The CPU (in cores) that needs to be allocated per source instance (applicable only to Docker runtime).
| `--deserialization-classname` | The SerDe classname for the source.
| `--destination-topic-name` | The Pulsar topic to which data is sent.
| `--disk` | The disk (in bytes) that needs to be allocated per source instance (applicable only to Docker runtime).
|`--name` | The source's name.
| `--namespace` | The source's namespace.
| ` --parallelism` | The source's parallelism factor, that is, the number of source instances to run.
| `--processing-guarantees` | The processing guarantees (also named as delivery semantics) applied to the source. A source connector receives messages from external system and writes messages to a Pulsar topic. The `--processing-guarantees` is used to ensure the processing guarantees for writing messages to the Pulsar topic. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
| `--ram` | The RAM (in bytes) that needs to be allocated per source instance (applicable only to the process and Docker runtimes).
| `-st`, `--schema-type` | The schema type.<br /> Either a builtin schema (for example, AVRO and JSON) or custom schema class name to be used to encode messages emitted from source.
| `--source-config` | Source config key/values.
| `--source-config-file` | The path to a YAML config file specifying the source's configuration.
| `-t`, `--source-type` | The source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.
| `--tenant` | The source's tenant.
| `--update-auth-data` | Whether or not to update the auth data.<br />**Default value: false.**


### `delete`

Delete a Pulsar IO source connector.

#### Usage

```bash

$ pulsar-admin sources delete options

```

#### Option

|Flag|Description|
|---|---|
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|

### `get`

Get the information about a Pulsar IO source connector.

#### Usage

```bash

$ pulsar-admin sources get options

```

#### Options
|Flag|Description|
|---|---|
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|


### `status`

Check the current status of a Pulsar Source.

#### Usage

```bash

$ pulsar-admin sources status options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The source ID.<br />If `instance-id` is not provided, Pulasr gets status of all instances.|
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|

### `list`

List all running Pulsar IO source connectors.

#### Usage

```bash

$ pulsar-admin sources list options

```

#### Options

|Flag|Description|
|---|---|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|


### `stop`

Stop a source instance.

#### Usage

```bash

$ pulsar-admin sources stop options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The source instanceID.<br />If `instance-id` is not provided, Pulsar stops all instances.|
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|

### `start`

Start a source instance.

#### Usage

```bash

$ pulsar-admin sources start options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The source instanceID.<br />If `instance-id` is not provided, Pulsar starts all instances.|
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|


### `restart`

Restart a source instance.

#### Usage

```bash

$ pulsar-admin sources restart options

```

#### Options
|Flag|Description|
|---|---|
|`--instance-id`|The source instanceID.<br />If `instance-id` is not provided, Pulsar restarts all instances.
|`--name`|The source's name.|
|`--namespace`|The source's namespace.|
|`--tenant`|The source's tenant.|


### `localrun`

Run a Pulsar IO source connector locally rather than deploying it to the Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sources localrun options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the NAR archive for the Source. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--broker-service-url` | The URL for the Pulsar broker.
|`--classname`|The source's class name if `archive` is file-url-path (file://).
| `--client-auth-params` | Client authentication parameter.
| `--client-auth-plugin` | Client authentication plugin using which function-process can connect to broker.
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime).|
|`--deserialization-classname`|The SerDe classname for the source.
|`--destination-topic-name`|The Pulsar topic to which data is sent.
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime).|
|`--hostname-verification-enabled`|Enable hostname verification.<br />**Default value: false**.
|`--name`|The source’s name.|
|`--namespace`|The source’s namespace.|
|`--parallelism`|The source’s parallelism factor, that is, the number of source instances to run).|
|`--processing-guarantees` | The processing guarantees (also named as delivery semantics) applied to the source. A source connector receives messages from external system and writes messages to a Pulsar topic. The `--processing-guarantees` is used to ensure the processing guarantees for writing messages to the Pulsar topic. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime).|
| `-st`, `--schema-type` | The schema type.<br /> Either a builtin schema (for example, AVRO and JSON) or custom schema class name to be used to encode messages emitted from source.
|`--source-config`|Source config key/values.
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration.
|`--source-type`|The source's connector provider.
|`--tenant`|The source’s tenant.
|`--tls-allow-insecure`|Allow insecure tls connection.<br />**Default value: false**.
|`--tls-trust-cert-path`|The tls trust cert file path.
|`--use-tls`|Use tls connection.<br />**Default value: false**.
|`--producer-config`| The custom producer configuration (as a JSON string).

### `available-sources`

Get the list of Pulsar IO connector sources supported by Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sources available-sources

```

### `reload`

Reload the available built-in connectors.

#### Usage

```bash

$ pulsar-admin sources reload

```

## `sinks`

An interface for managing Pulsar IO sinks (egress data from Pulsar).

```bash

$ pulsar-admin sinks subcommands

```

Subcommands are:

* `create`
  
* `update`
  
* `delete`
  
* `get`
  
* `status`
  
* `list`
  
* `stop`
  
* `start`
  
* `restart`
  
* `localrun`
  
* `available-sinks`

* `reload`


### `create`

Submit a Pulsar IO sink connector to run in a Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sinks create options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the archive file for the sink. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--auto-ack` |  Whether or not the framework will automatically acknowledge messages.
| `--classname` | The sink's class name if `archive` is file-url-path (file://).
| `--cpu` | The CPU (in cores) that needs to be allocated per sink instance (applicable only to Docker runtime).
| `--custom-schema-inputs` | The map of input topics to schema types or class names (as a JSON string).
| `--custom-serde-inputs` | The map of input topics to SerDe class names (as a JSON string).
| `--disk` | The disk (in bytes) that needs to be allocated per sink instance (applicable only to Docker runtime).
|`-i, --inputs` | The sink's input topic or topics (multiple topics can be specified as a comma-separated list).
|`--name` | The sink's name.
| `--namespace` | The sink's namespace.
| ` --parallelism` | The sink's parallelism factor, that is, the number of sink instances to run.
| `--processing-guarantees` | The processing guarantees (also known as delivery semantics) applied to the sink. The `--processing-guarantees` implementation in Pulsar also relies on sink implementation. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
| `--ram` | The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the process and Docker runtimes).
| `--retain-ordering` | Sink consumes and sinks messages in order.
| `--sink-config` | sink config key/values.
| `--sink-config-file` | The path to a YAML config file specifying the sink's configuration.
| `-t`, `--sink-type` | The sink's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.
| `--subs-name` | Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer.
| `--tenant` | The sink's tenant.
| `--timeout-ms` | The message timeout in milliseconds.
| `--topics-pattern` | TopicsPattern to consume from list of topics under a namespace that match the pattern. <br />`--input` and `--topics-Pattern` are mutually exclusive. <br />Add SerDe class name for a pattern in `--customSerdeInputs` (supported for java fun only).

### `update`

Update a Pulsar IO sink connector.

#### Usage

```bash

$ pulsar-admin sinks update options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the archive file for the sink. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--auto-ack` |  Whether or not the framework will automatically acknowledge messages.
| `--classname` | The sink's class name if `archive` is file-url-path (file://).
| `--cpu` | The CPU (in cores) that needs to be allocated per sink instance (applicable only to Docker runtime).
| `--custom-schema-inputs` | The map of input topics to schema types or class names (as a JSON string).
| `--custom-serde-inputs` | The map of input topics to SerDe class names (as a JSON string).
| `--disk` | The disk (in bytes) that needs to be allocated per sink instance (applicable only to Docker runtime).
|`-i, --inputs` | The sink's input topic or topics (multiple topics can be specified as a comma-separated list).
|`--name` | The sink's name.
| `--namespace` | The sink's namespace.
| ` --parallelism` | The sink's parallelism factor, that is, the number of sink instances to run.
| `--processing-guarantees` | The processing guarantees (also known as delivery semantics) applied to the sink. The `--processing-guarantees` implementation in Pulsar also relies on sink implementation. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
| `--ram` | The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the process and Docker runtimes).
| `--retain-ordering` | Sink consumes and sinks messages in order.
| `--sink-config` | sink config key/values.
| `--sink-config-file` | The path to a YAML config file specifying the sink's configuration.
| `-t`, `--sink-type` | The sink's connector provider.
| `--subs-name` | Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer.
| `--tenant` | The sink's tenant.
| `--timeout-ms` | The message timeout in milliseconds.
| `--topics-pattern` | TopicsPattern to consume from list of topics under a namespace that match the pattern. <br />`--input` and `--topics-Pattern` are mutually exclusive. <br />Add SerDe class name for a pattern in `--customSerdeInputs` (supported for java fun only).
| `--update-auth-data` | Whether or not to update the auth data.<br />**Default value: false.**

### `delete`

Delete a Pulsar IO sink connector.

#### Usage

```bash

$ pulsar-admin sinks delete options

```

#### Option

|Flag|Description|
|---|---|
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|

### `get`

Get the information about a Pulsar IO sink connector.

#### Usage

```bash

$ pulsar-admin sinks get options

```

#### Options
|Flag|Description|
|---|---|
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|


### `status`

Check the current status of a Pulsar sink.

#### Usage

```bash

$ pulsar-admin sinks status options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The sink ID.<br />If `instance-id` is not provided, Pulasr gets status of all instances.|
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|


### `list`

List all running Pulsar IO sink connectors.

#### Usage

```bash

$ pulsar-admin sinks list options

```

#### Options

|Flag|Description|
|---|---|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|


### `stop`

Stop a sink instance.

#### Usage

```bash

$ pulsar-admin sinks stop options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The sink instanceID.<br />If `instance-id` is not provided, Pulsar stops all instances.|
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|

### `start`

Start a sink instance.

#### Usage

```bash

$ pulsar-admin sinks start options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The sink instanceID.<br />If `instance-id` is not provided, Pulsar starts all instances.|
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|


### `restart`

Restart a sink instance.

#### Usage

```bash

$ pulsar-admin sinks restart options

```

#### Options

|Flag|Description|
|---|---|
|`--instance-id`|The sink instanceID.<br />If `instance-id` is not provided, Pulsar restarts all instances.
|`--name`|The sink's name.|
|`--namespace`|The sink's namespace.|
|`--tenant`|The sink's tenant.|


### `localrun`

Run a Pulsar IO sink connector locally rather than deploying it to the Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sinks localrun options

```

#### Options

|Flag|Description|
|----|---|
| `-a`, `--archive` | The path to the archive file for the sink. <br /> It also supports url-path (http/https/file [file protocol assumes that file already exists on worker host]) from which worker can download the package.
| `--auto-ack` | Whether or not the framework will automatically acknowledge messages.
| `--broker-service-url` | The URL for the Pulsar broker.
|`--classname`|The sink's class name if `archive` is file-url-path (file://).
| `--client-auth-params` | Client authentication parameter.
| `--client-auth-plugin` | Client authentication plugin using which function-process can connect to broker.
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime).
| `--custom-schema-inputs` | The map of input topics to Schema types or class names (as a JSON string).
| `--max-redeliver-count` | Maximum number of times that a message is redelivered before being sent to the dead letter queue.
| `--dead-letter-topic` | Name of the dead letter topic where the failing messages are sent.
| `--custom-serde-inputs` | The map of input topics to SerDe class names (as a JSON string).
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime).|
|`--hostname-verification-enabled`|Enable hostname verification.<br />**Default value: false**.
| `-i`, `--inputs` | The sink's input topic or topics (multiple topics can be specified as a comma-separated list).
|`--name`|The sink’s name.|
|`--namespace`|The sink’s namespace.|
|`--parallelism`|The sink’s parallelism factor, that is, the number of sink instances to run).|
|`--processing-guarantees`|The processing guarantees (also known as delivery semantics) applied to the sink. The `--processing-guarantees` implementation in Pulsar also relies on sink implementation. <br />The available values are ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime).|
|`--retain-ordering` | Sink consumes and sinks messages in order.
|`--sink-config`|sink config key/values.
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration.
|`--sink-type`|The sink's connector provider.
|`--subs-name` | Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer.
|`--tenant`|The sink’s tenant.
| `--timeout-ms` | The message timeout in milliseconds.
| `--negative-ack-redelivery-delay-ms` | The negatively-acknowledged message redelivery delay in milliseconds. |
|`--tls-allow-insecure`|Allow insecure tls connection.<br />**Default value: false**.
|`--tls-trust-cert-path`|The tls trust cert file path.
| `--topics-pattern` | TopicsPattern to consume from list of topics under a namespace that match the pattern. <br />`--input` and `--topics-Pattern` are mutually exclusive. <br />Add SerDe class name for a pattern in `--customSerdeInputs` (supported for java fun only).
|`--use-tls`|Use tls connection.<br />**Default value: false**.

### `available-sinks`

Get the list of Pulsar IO connector sinks supported by Pulsar cluster.

#### Usage

```bash

$ pulsar-admin sinks available-sinks

```

### `reload`

Reload the available built-in connectors.

#### Usage

```bash

$ pulsar-admin sinks reload

```

