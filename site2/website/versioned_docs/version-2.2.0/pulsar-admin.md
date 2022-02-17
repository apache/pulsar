---
id: pulsar-admin
title: Pulsar admin CLI
sidebar_label: "Pulsar Admin CLI"
original_id: pulsar-admin
---

The `pulsar-admin` tool enables you to manage Pulsar installations, including clusters, brokers, namespaces, tenants, and more.

Usage

```bash

$ pulsar-admin command

```

Commands
* `broker-stats`
* `brokers`
* `clusters`
* `functions`
* `namespaces`
* `ns-isolation-policy`
* `sink`
* `source`
* `topics`
* `tenants`
* `resource-quotas`
* `schemas`

## `broker-stats`

Operations to collect broker statistics

```bash

$ pulsar-admin broker-stats subcommand

```

Subcommands
* `allocator-stats`
* `destinations`
* `mbeans`
* `monitoring-metrics`
* `topics`


### `allocator-stats`

Dump allocator stats

Usage

```bash

$ pulsar-admin broker-stats allocator-stats allocator-name

```

### `destinations`

Dump topic stats

Usage

```bash

$ pulsar-admin broker-stats destinations options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--indent`|Indent JSON output|false|

### `mbeans`

Dump Mbean stats

Usage

```bash

$ pulsar-admin broker-stats mbeans options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--indent`|Indent JSON output|false|


### `monitoring-metrics`

Dump metrics for monitoring

Usage

```bash

$ pulsar-admin broker-stats monitoring-metrics options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--indent`|Indent JSON output|false|


### `topics`

Dump topic stats

Usage

```bash

$ pulsar-admin broker-stats topics options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-i`, `--indent`|Indent JSON output|false|


## `brokers`

Operations about brokers

```bash

$ pulsar-admin brokers subcommand

```

Subcommands
* `list`
* `namespaces`
* `update-dynamic-config`
* `list-dynamic-config`
* `get-all-dynamic-config`
* `get-internal-config`

### `list`
List active brokers of the cluster

Usage

```bash

$ pulsar-admin brokers list cluster-name

```

### `namespaces`
List namespaces owned by the broker

Usage

```bash

$ pulsar-admin brokers namespaces cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--url`|The URL for the broker||


### `update-dynamic-config`
Update a broker's dynamic service configuration

Usage

```bash

$ pulsar-admin brokers update-dynamic-config options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--config`|Service configuration parameter name||
|`--value`|Value for the configuration parameter value specified using the `--config` flag||


### `list-dynamic-config`
Get list of updatable configuration name

Usage

```bash

$ pulsar-admin brokers list-dynamic-config

```

### `get-all-dynamic-config`
Get all overridden dynamic-configuration values

Usage

```bash

$ pulsar-admin brokers get-all-dynamic-config

```

### `get-internal-config`
Get internal configuration information

Usage

```bash

$ pulsar-admin brokers get-internal-config

```

## `clusters`
Operations about clusters

Usage

```bash

$ pulsar-admin clusters subcommand

```

Subcommands
* `get`
* `create`
* `update`
* `delete`
* `list`
* `update-peer-clusters`


### `get`
Get the configuration data for the specified cluster

Usage

```bash

$ pulsar-admin clusters get cluster-name

```

### `create`
Provisions a new cluster. This operation requires Pulsar super-user privileges.

Usage

```bash

$ pulsar-admin clusters create cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--broker-url`|The URL for the broker service.||
|`--broker-url-secure`|The broker service URL for a secure connection||
|`--url`|service-url||
|`--url-secure`|service-url for secure connection||


### `update`
Update the configuration for a cluster

Usage

```bash

$ pulsar-admin clusters update cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--broker-url`|The URL for the broker service.||
|`--broker-url-secure`|The broker service URL for a secure connection||
|`--url`|service-url||
|`--url-secure`|service-url for secure connection||


### `delete`
Deletes an existing cluster

Usage

```bash

$ pulsar-admin clusters delete cluster-name

```

### `list`
List the existing clusters

Usage

```bash

$ pulsar-admin clusters list

```

### `update-peer-clusters`
Update peer cluster names

Usage

```bash

$ pulsar-admin clusters update-peer-clusters peer-cluster-names

```

## `functions`

A command-line interface for Pulsar Functions

Usage

```bash

$ pulsar-admin functions subcommand

```

Subcommands
* `localrun`
* `create`
* `delete`
* `update`
* `get`
* `getstatus`
* `list`
* `querystate`
* `trigger`


### `localrun`
Run a Pulsar Function locally


Usage

```bash

$ pulsar-admin functions localrun options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The CPU to allocate to each function instance (in number of cores)||
|`--ram`|The RAM to allocate to each function instance (in bytes)||
|`--disk`|The disk space to allocate to each function instance (in bytes)||
|`--auto-ack`|Let the functions framework manage acking||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--broker-service-url `|The URL of the Pulsar broker||
|`--classname`|The name of the function’s class||
|`--custom-serde-inputs`|A map of the input topic to SerDe name||
|`--custom-schema-inputs`|A map of the input topic to Schema class name||
|`--client-auth-params`|Client Authentication Params||
|`--function-config-file`|The path of the YAML config file used to configure the function||
|`--hostname-verification-enabled`|Enable Hostname verification||
|`--instance-id-offset`|Instance ids will be assigned starting from this offset||
|`--inputs`|The input topics for the function (as a comma-separated list if more than one topic is desired)||
|`--log-topic`|The topic to which logs from this function are published||
|`--jar`|A path to the JAR file for the function (if the function is written in Java)||
|`--name`|The name of the function||
|`--namespace`|The function’s namespace||
|`--output`|The name of the topic to which the function publishes its output (if any)||
|`--output-serde-classname`|The SerDe class used for the function’s output||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees applied to the function. Can be one of: ATLEAST_ONCE, ATMOST_ONCE, or EFFECTIVELY_ONCE|ATLEAST_ONCE|
|`--py`|The path of the Python file containing the function’s processing logic (if the function is written in Python)||
|`--schema-type`|Schema Type to be used for storing output messages||
|`--sliding-interval-count`|Number of messages after which the window ends||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--state-storage-service-url`|The service URL for the function’s state storage (if the function uses a storage system different from the Apache BookKeeper cluster used by Pulsar)||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern||
|`--user-config`|A user-supplied config value, set as a key/value pair. You can set multiple user config values.||
|`--window-length-count`|The number of messages per window.||
|`--window-length-duration-ms`|The time duration of the window in milliseconds.||


### `create`
Creates a new Pulsar Function on the target infrastructure

Usage

```

$ pulsar-admin functions create options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The CPU to allocate to each function instance (in number of cores)||
|`--ram`|The RAM to allocate to each function instance (in bytes)||
|`--disk`|The disk space to allocate to each function instance (in bytes)||
|`--auto-ack`|Let the functions framework manage acking||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--classname`|The name of the function’s class||
|`--custom-serde-inputs`|A map of the input topic to SerDe name||
|`--custom-schema-inputs`|A map of the input topic to Schema class name||
|`--function-config-file`|The path of the YAML config file used to configure the function||
|`--inputs`|The input topics for the function (as a comma-separated list if more than one topic is desired)||
|`--log-topic`|The topic to which logs from this function are published||
|`--jar`|A path to the JAR file for the function (if the function is written in Java)||
|`--name`|The name of the function||
|`--namespace`|The function’s namespace||
|`--output`|The name of the topic to which the function publishes its output (if any)||
|`--output-serde-classname`|The SerDe class used for the function’s output||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees applied to the function. Can be one of: ATLEAST_ONCE, ATMOST_ONCE, or EFFECTIVELY_ONCE|ATLEAST_ONCE|
|`--py`|The path of the Python file containing the function’s processing logic (if the function is written in Python)||
|`--schema-type`|Schema Type to be used for storing output messages||
|`--sliding-interval-count`|Number of messages after which the window ends||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern||
|`--user-config`|A user-supplied config value, set as a key/value pair. You can set multiple user config values.||
|`--window-length-count`|The number of messages per window.||
|`--window-length-duration-ms`|The time duration of the window in milliseconds.||


### `delete`
Deletes an existing Pulsar Function

Usage

```bash

$ pulsar-admin functions delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function to delete||
|`--namespace`|The namespace of the function to delete||
|`--tenant`|The tenant of the function to delete||


### `update`
Updates an existing Pulsar Function

Usage

```bash

$ pulsar-admin functions update options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The CPU to allocate to each function instance (in number of cores)||
|`--ram`|The RAM to allocate to each function instance (in bytes)||
|`--disk`|The disk space to allocate to each function instance (in bytes)||
|`--auto-ack`|Let the functions framework manage acking||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--classname`|The name of the function’s class||
|`--custom-serde-inputs`|A map of the input topic to SerDe name||
|`--custom-schema-inputs`|A map of the input topic to Schema class name||
|`--function-config-file`|The path of the YAML config file used to configure the function||
|`--inputs`|The input topics for the function (as a comma-separated list if more than one topic is desired)||
|`--log-topic`|The topic to which logs from this function are published||
|`--jar`|A path to the JAR file for the function (if the function is written in Java)||
|`--name`|The name of the function||
|`--namespace`|The function’s namespace||
|`--output`|The name of the topic to which the function publishes its output (if any)||
|`--output-serde-classname`|The SerDe class used for the function’s output||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees applied to the function. Can be one of: ATLEAST_ONCE, ATMOST_ONCE, or EFFECTIVELY_ONCE|ATLEAST_ONCE|
|`--py`|The path of the Python file containing the function’s processing logic (if the function is written in Python)||
|`--schema-type`|Schema Type to be used for storing output messages||
|`--sliding-interval-count`|Number of messages after which the window ends||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern||
|`--user-config`|A user-supplied config value, set as a key/value pair. You can set multiple user config values.||
|`--window-length-count`|The number of messages per window.||
|`--window-length-duration-ms`|The time duration of the window in milliseconds.||


### `get`
Fetch information about an existing Pulsar Function

Usage

```bash

$ pulsar-admin functions get options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function||
|`--namespace`|The namespace of the function||
|`--tenant`|The tenant of the function||


### `restart`
Restarts either all instances or one particular instance of a function

Usage

```bash

$ pulsar-admin functions restart options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function||
|`--namespace`|The namespace of the function||
|`--tenant`|The tenant of the function||
|`--instance-id`|The function instanceId; restart all instances if instance-id is not provided||


### `stop`
Temporary stops function instance. (If worker restarts then it reassigns and starts functiona again)

Usage

```bash

$ pulsar-admin functions stop options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function||
|`--namespace`|The namespace of the function||
|`--tenant`|The tenant of the function||
|`--instance-id`|The function instanceId; stop all instances if instance-id is not provided||


### `getstatus`
Get the status of an existing Pulsar Function

Usage

```bash

$ pulsar-admin functions getstatus options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function||
|`--namespace`|The namespace of the function||
|`--tenant`|The tenant of the function||
|`--instance-id`|The function instanceId; get status of all instances if instance-id is not provided||

### `list`
List all Pulsar Functions for a specific tenant and namespace

Usage

```bash

$ pulsar-admin functions list options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--namespace`|The namespace of the function||
|`--tenant`|The tenant of the function||


### `querystate`
Retrieve the current state of a Pulsar Function by key

Usage

```bash

$ pulsar-admin functions querystate options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-k`, `--key`|The key for the state you want to fetch||
|`--name`|The name of the function whose state you want to query||
|`--namespace`|The namespace of the function whose state you want to query||
|`--tenant`|The tenant of the function whose state you want to query||
|`-u`, `--storage-service-url`|The service URL for the function’s state storage (if the function uses a storage system different from the Apache BookKeeper cluster used by Pulsar)||
|`-w`, `--watch`|If set, watching for state changes is enabled|false|


### `trigger`
Triggers the specified Pulsar Function with a supplied value or file data

Usage

```bash

$ pulsar-admin functions trigger options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the Pulsar Function to trigger||
|`--namespace`|The namespace of the Pulsar Function to trigger||
|`--tenant`|The tenant of the Pulsar Function to trigger||
|`--trigger-file`|The path to the file containing the data with which the Pulsar Function is to be triggered||
|`--trigger-value`|The value with which the Pulsar Function is to be triggered||


## `namespaces`

Operations for managing namespaces

```bash

$ pulsar-admin namespaces subcommand

```

Subcommands
* `list`
* `list-cluster`
* `destinations`
* `policies`
* `create`
* `delete`
* `set-deduplication`
* `permissions`
* `grant-permission`
* `revoke-permission`
* `set-clusters`
* `get-clusters`
* `get-backlog-quotas`
* `set-backlog-quota`
* `remove-backlog-quota`
* `get-persistence`
* `set-persistence`
* `get-message-ttl`
* `set-message-ttl`
* `get-retention`
* `set-retention`
* `unload`
* `clear-backlog`
* `unsubscribe`
* `get-compaction-threshold`
* `set-compaction-threshold`
* `get-offload-threshold`
* `set-offload-threshold`


### `list`
Get the namespaces for a tenant

Usage

```bash

$ pulsar-admin namespaces list tenant-name

```

### `list-cluster`
Get the namespaces for a tenant in the cluster

Usage

```bash

$ pulsar-admin namespaces list-cluster tenant/cluster

```

### `destinations`
Get the destinations for a namespace

Usage

```bash

$ pulsar-admin namespaces destinations tenant/cluster/namespace

```

### `policies`
Get the policies of a namespace

Usage

```bash

$ pulsar-admin namespaces policies tenant/cluster/namespace

```

### `create`
Create a new namespace

Usage

```bash

$ pulsar-admin namespaces create tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-b` , `--bundles`|The number of bundles to activate|0|


### `delete`
Deletes a namespace

Usage

```bash

$ pulsar-admin namespaces delete tenant/cluster/namespace

```

### `set-deduplication`
Enable or disable message deduplication on a namespace

Usage

```bash

$ pulsar-admin namespaces set-deduplication tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--enable`, `-e`|Enable message deduplication on the specified namespace|false|
|`--disable`, `-d`|Disable message deduplication on the specified namespace|false|


### `permissions`
Get the permissions on a namespace

Usage

```bash

$ pulsar-admin namespaces permissions tenant/cluster/namespace

```

### `grant-permission`
Grant permissions on a namespace

Usage

```bash

$ pulsar-admin namespaces grant-permission tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--actions`|Actions to be granted (`produce` or `consume`)||
|`--role`|The client role to which to grant the permissions||


### `revoke-permission`
Revoke permissions on a namespace

Usage

```bash

$ pulsar-admin namespaces revoke-permission tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--role`|The client role to which to grant the permissions||


### `set-clusters`
Set replication clusters for a namespace

Usage

```bash

$ pulsar-admin namespaces set-clusters tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--clusters`|Replication clusters ID list (comma-separated values)||


### `get-clusters`
Get replication clusters for a namespace

Usage

```bash

$ pulsar-admin namespaces get-clusters tenant/cluster/namespace

```

### `get-backlog-quotas`
Get the backlog quota policies for a namespace

Usage

```bash

$ pulsar-admin namespaces get-backlog-quotas tenant/cluster/namespace

```

### `set-backlog-quota`
Set a backlog quota for a namespace

Usage

```bash

$ pulsar-admin namespaces set-backlog-quota tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-l`, `--limit`|The backlog size limit (for example `10M` or `16G`)||
|`-p`, `--policy`|The retention policy to enforce when the limit is reached. The valid options are: `producer_request_hold`, `producer_exception` or `consumer_backlog_eviction`|

Example

```bash

$ pulsar-admin namespaces set-backlog-quota my-prop/my-cluster/my-ns \
--limit 2G \
--policy producer_request_hold

```

### `remove-backlog-quota`
Remove a backlog quota policy from a namespace

Usage

```bash

$ pulsar-admin namespaces remove-backlog-quota tenant/cluster/namespace

```

### `get-persistence`
Get the persistence policies for a namespace

Usage

```bash

$ pulsar-admin namespaces get-persistence tenant/cluster/namespace

```

### `set-persistence`
Set the persistence policies for a namespace

Usage

```bash

$ pulsar-admin namespaces set-persistence tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-a`, `--bookkeeper-ack-quorum`|The number of acks (guaranteed copies) to wait for each entry|0|
|`-e`, `--bookkeeper-ensemble`|The number of bookies to use for a topic|0|
|`-w`, `--bookkeeper-write-quorum`|How many writes to make of each entry|0|
|`-r`, `--ml-mark-delete-max-rate`|Throttling rate of mark-delete operation (0 means no throttle)||


### `get-message-ttl`
Get the message TTL for a namespace

Usage

```bash

$ pulsar-admin namespaces get-message-ttl tenant/cluster/namespace

```

### `set-message-ttl`
Set the message TTL for a namespace

Usage

```bash

$ pulsar-admin namespaces set-message-ttl options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-ttl`, `--messageTTL`|Message TTL in seconds. When the value is set to `0`, TTL is disabled. TTL is disabled by default.|0|


### `get-retention`
Get the retention policy for a namespace

Usage

```bash

$ pulsar-admin namespaces get-retention tenant/cluster/namespace

```

### `set-retention`
Set the retention policy for a namespace

Usage

```bash

$ pulsar-admin namespaces set-retention tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-s`, `--size`|The retention size limits (for example 10M, 16G or 3T). 0 means no retention and -1 means infinite size retention||
|`-t`, `--time`|The retention time in minutes, hours, days, or weeks. Examples: 100m, 13h, 2d, 5w. 0 means no retention and -1 means infinite time retention||


### `unload`
Unload a namespace or namespace bundle from the current serving broker.

Usage

```bash

$ pulsar-admin namespaces unload tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|||


### `clear-backlog`
Clear the backlog for a namespace

Usage

```bash

$ pulsar-admin namespaces clear-backlog tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|||   
|`-f`, `--force`|Whether to force a clear backlog without prompt|false|
|`-s`, `--sub`|The subscription name||


### `unsubscribe`
Unsubscribe the given subscription on all destinations on a namespace

Usage

```bash

$ pulsar-admin namespaces unsubscribe tenant/cluster/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|||   
|`-s`, `--sub`|The subscription name||


### `get-compaction-threshold`
Get compactionThreshold for a namespace

Usage

```bash

$ pulsar-admin namespaces get-compaction-threshold tenant/namespace

```

### `set-compaction-threshold`
Set compactionThreshold for a namespace

Usage

```bash

$ pulsar-admin namespaces set-compaction-threshold tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-t`, `--threshold`|Maximum number of bytes in a topic backlog before compaction is triggered (eg: 10M, 16G, 3T). 0 disables automatic compaction|0|


### `get-offload-threshold`
Get offloadThreshold for a namespace

Usage

```bash

$ pulsar-admin namespaces get-offload-threshold tenant/namespace

```

Options

|Flag|Description|Default|
|----|---|---|
|`-s`, `--size`|Maximum number of bytes stored in the pulsar cluster for a topic before data will start being automatically offloaded to longterm storage (eg: 10M, 16G, 3T, 100). Negative values disable automatic offload. 0 triggers offloading as soon as possible.|-1|



## `ns-isolation-policy`
Operations for managing namespace isolation policies.

Usage

```bash

$ pulsar-admin ns-isolation-policy subcommand

```

Subcommands
* `set`
* `get`
* `list`
* `delete`

### `set`
Create/update a namespace isolation policy for a cluster. This operation requires Pulsar superuser privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy set cluster-name policy-name options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--auto-failover-policy-params`|Comma-separated name=value auto failover policy parameters|[]|
|`--auto-failover-policy-type`|Auto failover policy type name. Currently available options: min_available.|[]|
|`--namespaces`|Comma-separated namespaces regex list|[]|
|`--primary`|Comma-separated primary broker regex list|[]|
|`--secondary`|Comma-separated secondary broker regex list|[]|


### `get`
Get the namespace isolation policy of a cluster. This operation requires Pulsar superuser privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy get cluster-name policy-name

```

### `list`
List all namespace isolation policies of a cluster. This operation requires Pulsar superuser privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy list cluster-name

```

### `delete`
Delete namespace isolation policy of a cluster. This operation requires superuser privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy delete

```

## `sink`

An interface for managing Pulsar IO sinks (egress data from Pulsar)

Usage

```bash

$ pulsar-admin sink subcommand

```

Subcommands
* `create`
* `update`
* `delete`
* `localrun`
* `available-sinks`


### `create`
Submit a Pulsar IO sink connector to run in a Pulsar cluster

Usage

```bash

$ pulsar-admin sink create options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The sink’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|“The sink’s parallelism factor (i.e. the number of sink instances to run).”||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|Sink config key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern.||
|`--tenant`|The sink’s tenant||
|`--auto-ack`|Let the functions framework manage acking||
|`--timeout-ms`|The message timeout in milliseconds||


### `update`
Submit a Pulsar IO sink connector to run in a Pulsar cluster

Usage

```bash

$ pulsar-admin sink update options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The sink’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|“The sink’s parallelism factor (i.e. the number of sink instances to run).”||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|Sink config key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern.||
|`--tenant`|The sink’s tenant||


### `delete`
Stops a Pulsar IO sink

Usage

```bash

$ pulsar-admin sink delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function to delete||
|`--namespace`|The namespace of the function to delete||
|`--tenant`|The tenant of the function to delete||


### `localrun`
Run the Pulsar sink locally (rather than in the Pulsar cluster)

Usage

```bash

$ pulsar-admin sink localrun options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--broker-service-url`|The URL for the Pulsar broker||
|`--classname`|The sink’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|“The sink’s parallelism factor (i.e. the number of sink instances to run).”||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|Sink config key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern.||
|`--tenant`|The sink’s tenant||
|`--auto-ack`|Let the functions framework manage acking||
|`--timeout-ms`|The message timeout in milliseconds||


### `available-sinks`
Get a list of all built-in sink connectors

Usage

```bash

$ pulsar-admin sink available-sinks

```

## `source`
An interface for managing Pulsar IO sources (ingress data into Pulsar)

Usage

```bash

$ pulsar-admin source subcommand

```

Subcommands
* `create`
* `update`
* `delete`
* `localrun`
* `available-sources`


### `create`
Submit a Pulsar IO source connector to run in a Pulsar cluster

Usage

```bash

$ pulsar-admin source create options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file. ||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||


### `update`
Update a already submitted Pulsar IO source connector

Usage

```bash

$ pulsar-admin source update options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||


### `delete`
Stops a Pulsar IO source

Usage

```bash

$ pulsar-admin source delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The name of the function to delete||
|`--namespace`|The namespace of the function to delete||
|`--tenant`|The tenant of the function to delete||


### `localrun`
Run the Pulsar source locally (rather than in the Pulsar cluster)

Usage

```bash

$ pulsar-admin source localrun options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source’s Java class name||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|“The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.”||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||


### `available-sources`
Get a list of all built-in source connectors

Usage

```bash

$ pulsar-admin source available-sources

```

## `topics`
Operations for managing Pulsar topics (both persistent and non persistent)

Usage

```bash

$ pulsar-admin topics subcommand

```

Subcommands
* `compact`
* `compaction-status`
* `offload`
* `offload-status`
* `create-partitioned-topic`
* `delete-partitioned-topic`
* `get-partitioned-topic-metadata`
* `update-partitioned-topic`
* `list`
* `list-in-bundle`
* `terminate`
* `permissions`
* `grant-permission`
* `revoke-permission`
* `lookup`
* `bundle-range`
* `delete`
* `unload`
* `subscriptions`
* `unsubscribe`
* `stats`
* `stats-internal`
* `info-internal`
* `partitioned-stats`
* `skip`
* `skip-all`
* `expire-messages`
* `expire-messages-all-subscriptions`
* `peek-messages`
* `reset-cursor`


### `compact`
Run compaction on the specified topic (persistent topics only)

Usage

```

$ pulsar-admin topics compact persistent://tenant/namespace/topic

```

### `compaction-status`
Check the status of a topic compaction (persistent topics only)

Usage

```bash

$ pulsar-admin topics compaction-status persistent://tenant/namespace/topic

```

Options

|Flag|Description|Default|
|----|---|---|
|`-w`, `--wait-complete`|Wait for compaction to complete|false|


### `offload`
Trigger offload of data from a topic to long-term storage (e.g. Amazon S3)

Usage

```bash

$ pulsar-admin topics offload persistent://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-s`, `--size-threshold`|The maximum amount of data to keep in BookKeeper for the specific topic||


### `offload-status`
Check the status of data offloading from a topic to long-term storage

Usage

```bash

$ pulsar-admin topics offload-status persistent://tenant/namespace/topic op

```

Options

|Flag|Description|Default|
|---|---|---|
|`-w`, `--wait-complete`|Wait for compaction to complete|false|


### `create-partitioned-topic`
Create a partitioned topic. A partitioned topic must be created before producers can publish to it.

Usage

```bash

$ pulsar-admin topics create-partitioned-topic {persistent|non-persistent}://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-p`, `--partitions`|The number of partitions for the topic|0|


### `delete-partitioned-topic`
Delete a partitioned topic. This will also delete all the partitions of the topic if they exist.

Usage

```bash

$ pulsar-admin topics delete-partitioned-topic {persistent|non-persistent}

```

### `get-partitioned-topic-metadata`
Get the partitioned topic metadata. If the topic is not created or is a non-partitioned topic, this will return an empty topic with zero partitions.

Usage

```bash

$ pulsar-admin topics get-partitioned-topic-metadata {persistent|non-persistent}://tenant/namespace/topic

```

### `update-partitioned-topic`
Update existing non-global partitioned topic. New updating number of partitions must be greater than existing number of partitions.

Usage

```bash

$ pulsar-admin topics update-partitioned-topic {persistent|non-persistent}://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-p`, `--partitions`|The number of partitions for the topic|0|

### `list`
Get the list of topics under a namespace

Usage

```

$ pulsar-admin topics list tenant/cluster/namespace

```

### `list-in-bundle`
Get a list of non-persistent topics present under a namespace bundle

Usage

```

$ pulsar-admin topics list-in-bundle tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-b`, `--bundle`|The bundle range||


### `terminate`
Terminate a persistent topic (disallow further messages from being published on the topic)

Usage

```bash

$ pulsar-admin topics terminate persistent://tenant/namespace/topic

```

### `permissions`
Get the permissions on a topic. Retrieve the effective permissions for a destination. These permissions are defined by the permissions set at the namespace level combined (union) with any eventual specific permissions set on the topic.

Usage

```bash

$ pulsar-admin topics permissions topic

```

### `grant-permission`
Grant a new permission to a client role on a single topic

Usage

```bash

$ pulsar-admin topics grant-permission {persistent|non-persistent}://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--actions`|Actions to be granted (`produce` or `consume`)||
|`--role`|The client role to which to grant the permissions||


### `revoke-permission`
Revoke permissions to a client role on a single topic. If the permission was not set at the topic level, but rather at the namespace level, this operation will return an error (HTTP status code 412).

Usage

```bash

$ pulsar-admin topics revoke-permission topic

```

### `lookup`
Look up a topic from the current serving broker

Usage

```bash

$ pulsar-admin topics lookup topic

```

### `bundle-range`
Get the namespace bundle which contains the given topic

Usage

```bash

$ pulsar-admin topics bundle-range topic

```

### `delete`
Delete a topic. The topic cannot be deleted if there are any active subscriptions or producers connected to the topic.

Usage

```bash

$ pulsar-admin topics delete topic

```

### `unload`
Unload a topic

Usage

```bash

$ pulsar-admin topics unload topic

```

### `subscriptions`
Get the list of subscriptions on the topic

Usage

```bash

$ pulsar-admin topics subscriptions topic

```

### `unsubscribe`
Delete a durable subscriber from a topic

Usage

```bash

$ pulsar-admin topics unsubscribe topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-s`, `--subscription`|The subscription to delete||


### `stats`
Get the stats for the topic and its connected producers and consumers. All rates are computed over a 1-minute window and are relative to the last completed 1-minute period.

Usage

```bash

$ pulsar-admin topics stats topic

```

### `stats-internal`
Get the internal stats for the topic

Usage

```bash

$ pulsar-admin topics stats-internal topic

```

### `info-internal`
Get the internal metadata info for the topic

Usage

```bash

$ pulsar-admin topics info-internal topic

```

### `partitioned-stats`
Get the stats for the partitioned topic and its connected producers and consumers. All rates are computed over a 1-minute window and are relative to the last completed 1-minute period.

Usage

```bash

$ pulsar-admin topics partitioned-stats topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--per-partition`|Get per-partition stats|false|


### `skip`
Skip some messages for the subscription

Usage

```bash

$ pulsar-admin topics skip topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-n`, `--count`|The number of messages to skip|0|
|`-s`, `--subscription`|The subscription on which to skip messages||


### `skip-all`
Skip all the messages for the subscription

Usage

```bash

$ pulsar-admin topics skip-all topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-s`, `--subscription`|The subscription to clear||


### `expire-messages`
Expire messages that are older than the given expiry time (in seconds) for the subscription.

Usage

```bash

$ pulsar-admin topics expire-messages topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-t`, `--expireTime`|Expire messages older than the time (in seconds)|0|
|`-s`, `--subscription`|The subscription to skip messages on||


### `expire-messages-all-subscriptions`
Expire messages older than the given expiry time (in seconds) for all subscriptions

Usage

```bash

$ pulsar-admin topics expire-messages-all-subscriptions topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-t`, `--expireTime`|Expire messages older than the time (in seconds)|0|


### `peek-messages`
Peek some messages for the subscription.

Usage

```bash

$ pulsar-admin topics peek-messages topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-n`, `--count`|The number of messages|0|
|`-s`, `--subscription`|Subscription to get messages from||


### `reset-cursor`
Reset position for subscription to closest to timestamp

Usage

```bash

$ pulsar-admin topics reset-cursor topic options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-s`, `--subscription`|Subscription to reset position on||
|`-t`, `--time`|The time, in minutes, to reset back to (or minutes, hours, days, weeks, etc.). Examples: `100m`, `3h`, `2d`, `5w`.||



## `tenants`
Operations for managing tenants

Usage

```bash

$ pulsar-admin tenants subcommand

```

Subcommands
* `list`
* `get`
* `create`
* `update`
* `delete`

### `list`
List the existing tenants

Usage

```bash

$ pulsar-admin tenants list

```

### `get`
Gets the configuration of a tenant

Usage

```bash

$ pulsar-admin tenants get tenant-name

```

### `create`
Creates a new tenant

Usage

```bash

$ pulsar-admin tenants create tenant-name options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-r`, `--admin-roles`|Comma-separated admin roles||
|`-c`, `--allowed-clusters`|Comma-separated allowed clusters||

### `update`
Updates a tenant

Usage

```bash

$ pulsar-admin tenants update tenant-name options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-r`, `--admin-roles`|Comma-separated admin roles||
|`-c`, `--allowed-clusters`|Comma-separated allowed clusters||


### `delete`
Deletes an existing tenant

Usage

```bash

$ pulsar-admin tenants delete tenant-name

```

## `resource-quotas`
Operations for managing resource quotas

Usage

```bash

$ pulsar-admin resource-quotas subcommand

```

Subcommands
* `get`
* `set`
* `reset-namespace-bundle-quota`


### `get`
Get the resource quota for a specified namespace bundle, or default quota if no namespace/bundle is specified.

Usage

```bash

$ pulsar-admin resource-quotas get options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|A bundle of the form {start-boundary}_{end_boundary}. This must be specified together with -n/--namespace.||
|`-n`, `--namespace`|The namespace||


### `set`
Set the resource quota for the specified namespace bundle, or default quota if no namespace/bundle is specified.

Usage

```bash

$ pulsar-admin resource-quotas set options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-bi`, `--bandwidthIn`|The expected inbound bandwidth (in bytes/second)|0|
|`-bo`, `--bandwidthOut`|Expected outbound bandwidth (in bytes/second)0|
|`-b`, `--bundle`|A bundle of the form {start-boundary}_{end_boundary}. This must be specified together with -n/--namespace.||
|`-d`, `--dynamic`|Allow to be dynamically re-calculated (or not)|false|
|`-mem`, `--memory`|Expectred memory usage (in megabytes)|0|
|`-mi`, `--msgRateIn`|Expected incoming messages per second|0|
|`-mo`, `--msgRateOut`|Expected outgoing messages per second|0|
|`-n`, `--namespace`|The namespace as tenant/namespace, for example my-tenant/my-ns. Must be specified together with -b/--bundle.||


### `reset-namespace-bundle-quota`
Reset the specified namespace bundle's resource quota to a default value.

Usage

```bash

$ pulsar-admin resource-quotas reset-namespace-bundle-quota options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|A bundle of the form {start-boundary}_{end_boundary}. This must be specified together with -n/--namespace.||
|`-n`, `--namespace`|The namespace||



## `schemas`
Operations related to Schemas associated with Pulsar topics.

Usage

```

$ pulsar-admin schemas subcommand

```

Subcommands
* `upload`
* `delete`
* `get`


### `upload`
Upload the schema definition for a topic

Usage

```bash

$ pulsar-admin schemas upload persistent://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--filename`|The path to the schema definition file. An example schema file is available under conf directory.||


### `delete`
Delete the schema definition associated with a topic

Usage

```bash

$ pulsar-admin schemas delete persistent://tenant/namespace/topic

```

### `get`
Retrieve the schema definition associated with a topic (at a given version if version is supplied).

Usage

```bash

$ pulsar-admin schemas get persistent://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--version`|The version of the schema definition to retrieve for a topic.||


