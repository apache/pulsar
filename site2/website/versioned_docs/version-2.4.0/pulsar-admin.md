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
* `sinks`
* `sources`
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
* `topics(destinations)`
* `mbeans`
* `monitoring-metrics`
* `load-report`


### `allocator-stats`

Dump allocator stats

Usage

```bash

$ pulsar-admin broker-stats allocator-stats allocator-name

```

### `topics(destinations)`

Dump topic stats

Usage

```bash

$ pulsar-admin broker-stats topics options

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


### `load-report`

Dump broker load-report

Usage

```bash

$ pulsar-admin broker-stats load-report

```

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
* `get-runtime-config`
* `healthcheck`

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

### `get-runtime-config`
Get runtime configuration values

Usage

```bash

$ pulsar-admin brokers get-runtime-config

```

### `healthcheck`
Run a health check against the broker

Usage

```bash

$ pulsar-admin brokers healthcheck

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
* `get-peer-clusters`
* `get-failure-domain`
* `create-failure-domain`
* `update-failure-domain`
* `delete-failure-domain`
* `list-failure-domains`


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

$ pulsar-admin clusters update-peer-clusters cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--peer-clusters`|Comma separated peer cluster names (Pass empty string "" to delete list)||

### `get-peer-clusters`
Get list of peer clusters

Usage

```bash

$ pulsar-admin clusters get-peer-clusters

```

### `get-failure-domain`
Get the configuration brokers of a failure domain

Usage

```bash

$ pulsar-admin clusters get-failure-domain cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--domain-name`|The failure domain name, which is a logical domain under a Pulsar cluster||

### `create-failure-domain`
Create a new failure domain for a cluster (updates it if already created)

Usage

```bash

$ pulsar-admin clusters create-failure-domain cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--broker-list`|Comma separated broker list||
|`--domain-name`|The failure domain name, which is a logical domain under a Pulsar cluster||

### `update-failure-domain`
Update failure domain for a cluster (creates a new one if not exist)

Usage

```bash

$ pulsar-admin clusters update-failure-domain cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--broker-list`|Comma separated broker list||
|`--domain-name`|The failure domain name, which is a logical domain under a Pulsar cluster||

### `delete-failure-domain`
Delete an existing failure domain

Usage

```bash

$ pulsar-admin clusters delete-failure-domain cluster-name options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--domain-name`|The failure domain name, which is a logical domain under a Pulsar cluster||

### `list-failure-domains`
List the existing failure domains for a cluster

Usage

```bash

$ pulsar-admin clusters list-failure-domains cluster-name

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
* `restart`
* `stop`
* `start`
* `status`
* `stats`
* `list`
* `querystate`
* `trigger`


### `localrun`
Run the Pulsar Function locally (rather than deploying it to the Pulsar cluster)


Usage

```bash

$ pulsar-admin functions localrun options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)||
|`--ram`|The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)||
|`--disk`|The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--broker-service-url `|The URL of the Pulsar broker||
|`--classname`|The function's class name||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema class names (as a JSON string)||
|`--client-auth-params`|Client authentication param||
|`--client-auth-plugin`|Client authentication plugin using which function-process can connect to broker||
|`--function-config-file`|The path to a YAML config file specifying the function's configuration||
|`--hostname-verification-enabled`|Enable hostname verification|false|
|`--instance-id-offset`|Start the instanceIds from this offset|0|
|`--inputs`|The function's input topic or topics (multiple topics can be specified as a comma-separated list)||
|`--log-topic`|The topic to which the function's logs are produced||
|`--jar`|Path to the jar file for the function (if the function is written in Java). It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--output`|The function's output topic (If none is specified, no output is written)||
|`--output-serde-classname`|The SerDe class to be used for messages output by the function||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the function. Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]|ATLEAST_ONCE|
|`--py`|Path to the main Python file/Python Wheel file for the function (if the function is written in Python)||
|`--schema-type`|The builtin schema type or custom schema class name to be used for messages output by the function||
|`--sliding-interval-count`|The number of messages after which the window slides||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--state-storage-service-url`|The URL for the state storage service. By default, it it set to the service URL of the Apache BookKeeper. This service URL must be added manually when the Pulsar Function runs locally.||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)||
|`--user-config`|User-defined config key/values||
|`--window-length-count`|The number of messages per window||
|`--window-length-duration-ms`|The time duration of the window in milliseconds||
|`--dead-letter-topic`|The topic where all messages which could not be processed successfully are sent||
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--max-message-retries`|How many times should we try to process a message before giving up||
|`--retain-ordering`|Function consumes and processes messages in order||
|`--timeout-ms`|The message timeout in milliseconds||
|`--tls-allow-insecure`|Allow insecure tls connection|false|
|`--tls-trust-cert-path`|The tls trust cert file path||
|`--use-tls`|Use tls connection|false|


### `create`
Create a Pulsar Function in cluster mode (i.e. deploy it on a Pulsar cluster)

Usage

```

$ pulsar-admin functions create options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)||
|`--ram`|The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)||
|`--disk`|The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--classname`|The function's class name||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema class names (as a JSON string)||
|`--function-config-file`|The path to a YAML config file specifying the function's configuration||
|`--inputs`|The function's input topic or topics (multiple topics can be specified as a comma-separated list)||
|`--log-topic`|The topic to which the function's logs are produced||
|`--jar`|Path to the jar file for the function (if the function is written in Java). It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The function's name||
|`--namespace`|The function’s namespace||
|`--output`|The function's output topic (If none is specified, no output is written)||
|`--output-serde-classname`|The SerDe class to be used for messages output by the function||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the function. Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]|ATLEAST_ONCE|
|`--py`|Path to the main Python file/Python Wheel file for the function (if the function is written in Python)||
|`--schema-type`|The builtin schema type or custom schema class name to be used for messages output by the function||
|`--sliding-interval-count`|The number of messages after which the window slides||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)||
|`--user-config`|User-defined config key/values||
|`--window-length-count`|The number of messages per window||
|`--window-length-duration-ms`|The time duration of the window in milliseconds||
|`--dead-letter-topic`|The topic where all messages which could not be processed||
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--max-message-retries`|How many times should we try to process a message before giving up||
|`--retain-ordering`|Function consumes and processes messages in order||
|`--timeout-ms`|The message timeout in milliseconds||


### `delete`
Delete a Pulsar Function that's running on a Pulsar cluster

Usage

```bash

$ pulsar-admin functions delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `update`
Update a Pulsar Function that's been deployed to a Pulsar cluster

Usage

```bash

$ pulsar-admin functions update options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--cpu`|The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)||
|`--ram`|The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)||
|`--disk`|The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--classname`|The function's class name||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema class names (as a JSON string)||
|`--function-config-file`|The path to a YAML config file specifying the function's configuration||
|`--inputs`|The function's input topic or topics (multiple topics can be specified as a comma-separated list)||
|`--log-topic`|The topic to which the function's logs are produced||
|`--jar`|Path to the jar file for the function (if the function is written in Java). It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The function's name||
|`--namespace`|The function’s namespace||
|`--output`|The function's output topic (If none is specified, no output is written)||
|`--output-serde-classname`|The SerDe class to be used for messages output by the function||
|`--parallelism`|The function’s parallelism factor, i.e. the number of instances of the function to run|1|
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the function. Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]|ATLEAST_ONCE|
|`--py`|Path to the main Python file/Python Wheel file for the function (if the function is written in Python)||
|`--schema-type`|The builtin schema type or custom schema class name to be used for messages output by the function||
|`--sliding-interval-count`|The number of messages after which the window slides||
|`--sliding-interval-duration-ms`|The time duration after which the window slides||
|`--tenant`|The function’s tenant||
|`--topics-pattern`|The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)||
|`--user-config`|User-defined config key/values||
|`--window-length-count`|The number of messages per window||
|`--window-length-duration-ms`|The time duration of the window in milliseconds||
|`--dead-letter-topic`|The topic where all messages which could not be processed||
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--max-message-retries`|How many times should we try to process a message before giving up||
|`--retain-ordering`|Function consumes and processes messages in order||
|`--timeout-ms`|The message timeout in milliseconds||


### `get`
Fetch information about a Pulsar Function

Usage

```bash

$ pulsar-admin functions get options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `restart`
Restart function instance

Usage

```bash

$ pulsar-admin functions restart options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--instance-id`|The function instanceId (restart all instances if instance-id is not provided)||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `stop`
Stops function instance

Usage

```bash

$ pulsar-admin functions stop options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--instance-id`|The function instanceId (stop all instances if instance-id is not provided)||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `start`
Starts a stopped function instance

Usage

```bash

$ pulsar-admin functions start options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--instance-id`|The function instanceId (start all instances if instance-id is not provided)||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `status`
Check the current status of a Pulsar Function

Usage

```bash

$ pulsar-admin functions status options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--instance-id`|The function instanceId (Get-status of all instances if instance-id is not provided)||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `stats`
Get the current stats of a Pulsar Function

Usage

```bash

$ pulsar-admin functions stats options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--instance-id`|The function instanceId (Get-stats of all instances if instance-id is not provided)||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||

### `list`
List all of the Pulsar Functions running under a specific tenant and namespace

Usage

```bash

$ pulsar-admin functions list options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||


### `querystate`
Fetch the current state associated with a Pulsar Function running in cluster mode

Usage

```bash

$ pulsar-admin functions querystate options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`-k`, `--key`|The key for the state you want to fetch||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||
|`-w`, `--watch`|Watch for changes in the value associated with a key for a Pulsar Function|false|


### `trigger`
Triggers the specified Pulsar Function with a supplied value

Usage

```bash

$ pulsar-admin functions trigger options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--fqfn`|The Fully Qualified Function Name (FQFN) for the function||
|`--name`|The function's name||
|`--namespace`|The function's namespace||
|`--tenant`|The function's tenant||
|`--topic`|The specific topic name that the function consumes from that you want to inject the data to||
|`--trigger-file`|The path to the file that contains the data with which you'd like to trigger the function||
|`--trigger-value`|The value with which you want to trigger the function||


## `namespaces`

Operations for managing namespaces

```bash

$ pulsar-admin namespaces subcommand

```

Subcommands
* `list`
* `topics`
* `policies`
* `create`
* `delete`
* `set-deduplication`
* `permissions`
* `grant-permission`
* `revoke-permission`
* `grant-subscription-permission`
* `revoke-subscription-permission`
* `set-clusters`
* `get-clusters`
* `get-backlog-quotas`
* `set-backlog-quota`
* `remove-backlog-quota`
* `get-persistence`
* `set-persistence`
* `get-message-ttl`
* `set-message-ttl`
* `get-anti-affinity-group`
* `set-anti-affinity-group`
* `get-anti-affinity-namespaces`
* `delete-anti-affinity-group`
* `get-retention`
* `set-retention`
* `unload`
* `split-bundle`
* `set-dispatch-rate`
* `get-dispatch-rate`
* `set-subscribe-rate`
* `get-subscribe-rate`
* `set-subscription-dispatch-rate`
* `get-subscription-dispatch-rate`
* `clear-backlog`
* `unsubscribe`
* `set-encryption-required`
* `set-subscription-auth-mode`
* `get-max-producers-per-topic`
* `set-max-producers-per-topic`
* `get-max-consumers-per-topic`
* `set-max-consumers-per-topic`
* `get-max-consumers-per-subscription`
* `set-max-consumers-per-subscription`
* `get-compaction-threshold`
* `set-compaction-threshold`
* `get-offload-threshold`
* `set-offload-threshold`
* `get-offload-deletion-lag`
* `set-offload-deletion-lag`
* `clear-offload-deletion-lag`
* `get-schema-autoupdate-strategy`
* `set-schema-autoupdate-strategy`


### `list`
Get the namespaces for a tenant

Usage

```bash

$ pulsar-admin namespaces list tenant-name

```

### `topics`
Get the list of topics for a namespace

Usage

```bash

$ pulsar-admin namespaces topics tenant/namespace

```

### `policies`
Get the configuration policies of a namespace

Usage

```bash

$ pulsar-admin namespaces policies tenant/namespace

```

### `create`
Create a new namespace

Usage

```bash

$ pulsar-admin namespaces create tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-b`, `--bundles`|The number of bundles to activate|0|
|`-c`, `--clusters`|List of clusters this namespace will be assigned||


### `delete`
Deletes a namespace. The namespace needs to be empty

Usage

```bash

$ pulsar-admin namespaces delete tenant/namespace

```

### `set-deduplication`
Enable or disable message deduplication on a namespace

Usage

```bash

$ pulsar-admin namespaces set-deduplication tenant/namespace options

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

$ pulsar-admin namespaces permissions tenant/namespace

```

### `grant-permission`
Grant permissions on a namespace

Usage

```bash

$ pulsar-admin namespaces grant-permission tenant/namespace options

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

$ pulsar-admin namespaces revoke-permission tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--role`|The client role to which to revoke the permissions||

### `grant-subscription-permission`
Grant permissions to access subscription admin-api

Usage

```bash

$ pulsar-admin namespaces grant-subscription-permission tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--roles`|The client roles to which to grant the permissions (comma separated roles)||
|`--subscription`|The subscription name for which permission will be granted to roles||

### `revoke-subscription-permission`
Revoke permissions to access subscription admin-api

Usage

```bash

$ pulsar-admin namespaces revoke-subscription-permission tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--role`|The client role to which to revoke the permissions||
|`--subscription`|The subscription name for which permission will be revoked to roles||

### `set-clusters`
Set replication clusters for a namespace

Usage

```bash

$ pulsar-admin namespaces set-clusters tenant/namespace options

```

Options

|Flag|Description|Default|
|---|---|---|
|`-c`, `--clusters`|Replication clusters ID list (comma-separated values)||


### `get-clusters`
Get replication clusters for a namespace

Usage

```bash

$ pulsar-admin namespaces get-clusters tenant/namespace

```

### `get-backlog-quotas`
Get the backlog quota policies for a namespace

Usage

```bash

$ pulsar-admin namespaces get-backlog-quotas tenant/namespace

```

### `set-backlog-quota`
Set a backlog quota policy for a namespace

Usage

```bash

$ pulsar-admin namespaces set-backlog-quota tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-l`, `--limit`|The backlog size limit (for example `10M` or `16G`)||
|`-p`, `--policy`|The retention policy to enforce when the limit is reached. The valid options are: `producer_request_hold`, `producer_exception` or `consumer_backlog_eviction`|

Example

```bash

$ pulsar-admin namespaces set-backlog-quota my-tenant/my-ns \
--limit 2G \
--policy producer_request_hold

```

### `remove-backlog-quota`
Remove a backlog quota policy from a namespace

Usage

```bash

$ pulsar-admin namespaces remove-backlog-quota tenant/namespace

```

### `get-persistence`
Get the persistence policies for a namespace

Usage

```bash

$ pulsar-admin namespaces get-persistence tenant/namespace

```

### `set-persistence`
Set the persistence policies for a namespace

Usage

```bash

$ pulsar-admin namespaces set-persistence tenant/namespace options

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

$ pulsar-admin namespaces get-message-ttl tenant/namespace

```

### `set-message-ttl`
Set the message TTL for a namespace

Usage

```bash

$ pulsar-admin namespaces set-message-ttl tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-ttl`, `--messageTTL`|Message TTL in seconds. When the value is set to `0`, TTL is disabled. TTL is disabled by default.|0|

### `get-anti-affinity-group`
Get Anti-affinity group name for a namespace

Usage

```bash

$ pulsar-admin namespaces get-anti-affinity-group tenant/namespace

```

### `set-anti-affinity-group`
Set Anti-affinity group name for a namespace

Usage

```bash

$ pulsar-admin namespaces set-anti-affinity-group tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-g`, `--group`|Anti-affinity group name||

### `get-anti-affinity-namespaces`
Get Anti-affinity namespaces grouped with the given anti-affinity group name

Usage

```bash

$ pulsar-admin namespaces get-anti-affinity-namespaces options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-c`, `--cluster`|Cluster name||
|`-g`, `--group`|Anti-affinity group name||
|`-p`, `--tenant`|Tenant is only used for authorization. Client has to be admin of any of the tenant to access this api||

### `delete-anti-affinity-group`
Remove Anti-affinity group name for a namespace

Usage

```bash

$ pulsar-admin namespaces delete-anti-affinity-group tenant/namespace

```

### `get-retention`
Get the retention policy for a namespace

Usage

```bash

$ pulsar-admin namespaces get-retention tenant/namespace

```

### `set-retention`
Set the retention policy for a namespace

Usage

```bash

$ pulsar-admin namespaces set-retention tenant/namespace

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

$ pulsar-admin namespaces unload tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|{start-boundary}_{end-boundary} (e.g. 0x00000000_0xffffffff)||

### `split-bundle`
Split a namespace-bundle from the current serving broker

Usage

```bash

$ pulsar-admin namespaces split-bundle tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|{start-boundary}_{end-boundary} (e.g. 0x00000000_0xffffffff)||
|`-u`, `--unload`|Unload newly split bundles after splitting old bundle|false|

### `set-dispatch-rate`
Set message-dispatch-rate for all topics of the namespace

Usage

```bash

$ pulsar-admin namespaces set-dispatch-rate tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-bd`, `--byte-dispatch-rate`|The byte dispatch rate (default -1 will be overwrite if not passed)|-1|
|`-dt`, `--dispatch-rate-period`|The dispatch rate period in second type (default 1 second will be overwrite if not passed)|1|
|`-md`, `--msg-dispatch-rate`|The message dispatch rate (default -1 will be overwrite if not passed)|-1|

### `get-dispatch-rate`
Get configured message-dispatch-rate for all topics of the namespace (Disabled if value < 0)

Usage

```bash

$ pulsar-admin namespaces get-dispatch-rate tenant/namespace

```

### `set-subscribe-rate`
Set subscribe-rate per consumer for all topics of the namespace

Usage

```bash

$ pulsar-admin namespaces set-subscribe-rate tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-sr`, `--subscribe-rate`|The subscribe rate (default -1 will be overwrite if not passed)|-1|
|`-st`, `--subscribe-rate-period`|The subscribe rate period in second type (default 30 second will be overwrite if not passed)|30|

### `get-subscribe-rate`
Get configured subscribe-rate per consumer for all topics of the namespace

Usage

```bash

$ pulsar-admin namespaces get-subscribe-rate tenant/namespace

```

### `set-subscription-dispatch-rate`
Set subscription message-dispatch-rate for all subscription of the namespace

Usage

```bash

$ pulsar-admin namespaces set-subscription-dispatch-rate tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-bd`, `--byte-dispatch-rate`|The byte dispatch rate (default -1 will be overwrite if not passed)|-1|
|`-dt`, `--dispatch-rate-period`|The dispatch rate period in second type (default 1 second will be overwrite if not passed)|1|
|`-md`, `--sub-msg-dispatch-rate`|The message dispatch rate (default -1 will be overwrite if not passed)|-1|

### `get-subscription-dispatch-rate`
Get subscription configured message-dispatch-rate for all topics of the namespace (Disabled if value < 0)

Usage

```bash

$ pulsar-admin namespaces get-subscription-dispatch-rate tenant/namespace

```

### `clear-backlog`
Clear the backlog for a namespace

Usage

```bash

$ pulsar-admin namespaces clear-backlog tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|{start-boundary}_{end-boundary} (e.g. 0x00000000_0xffffffff)||
|`-force`, `--force`|Whether to force a clear backlog without prompt|false|
|`-s`, `--sub`|The subscription name||


### `unsubscribe`
Unsubscribe the given subscription on all destinations on a namespace

Usage

```bash

$ pulsar-admin namespaces unsubscribe tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-b`, `--bundle`|{start-boundary}_{end-boundary} (e.g. 0x00000000_0xffffffff)||
|`-s`, `--sub`|The subscription name||

### `set-encryption-required`
Enable or disable message encryption required for a namespace

Usage

```bash

$ pulsar-admin namespaces set-encryption-required tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-d`, `--disable`|Disable message encryption required|false|
|`-e`, `--enable`|Enable message encryption required|false|

### `set-subscription-auth-mode`
Set subscription auth mode on a namespace

Usage

```bash

$ pulsar-admin namespaces set-subscription-auth-mode tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-m`, `--subscription-auth-mode`|Subscription authorization mode for Pulsar policies. Valid options are: [None, Prefix]||

### `get-max-producers-per-topic`
Get maxProducersPerTopic for a namespace

Usage

```bash

$ pulsar-admin namespaces get-max-producers-per-topic tenant/namespace

```

### `set-max-producers-per-topic`
Set maxProducersPerTopic for a namespace

Usage

```bash

$ pulsar-admin namespaces set-max-producers-per-topic tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-p`, `--max-producers-per-topic`|maxProducersPerTopic for a namespace|0|

### `get-max-consumers-per-topic`
Get maxConsumersPerTopic for a namespace

Usage

```bash

$ pulsar-admin namespaces get-max-consumers-per-topic tenant/namespace

```

### `set-max-consumers-per-topic`
Set maxConsumersPerTopic for a namespace

Usage

```bash

$ pulsar-admin namespaces set-max-consumers-per-topic tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-c`, `--max-consumers-per-topic`|maxConsumersPerTopic for a namespace|0|

### `get-max-consumers-per-subscription`
Get maxConsumersPerSubscription for a namespace

Usage

```bash

$ pulsar-admin namespaces get-max-consumers-per-subscription tenant/namespace

```

### `set-max-consumers-per-subscription`
Set maxConsumersPerSubscription for a namespace

Usage

```bash

$ pulsar-admin namespaces set-max-consumers-per-subscription tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-c`, `--max-consumers-per-subscription`|maxConsumersPerSubscription for a namespace|0|


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

### `set-offload-threshold`
Set offloadThreshold for a namespace

Usage

```bash

$ pulsar-admin namespaces set-offload-threshold tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-s`, `--size`|Maximum number of bytes stored in the pulsar cluster for a topic before data will start being automatically offloaded to longterm storage (eg: 10M, 16G, 3T, 100). Negative values disable automatic offload. 0 triggers offloading as soon as possible.|-1|

### `get-offload-deletion-lag`
Get offloadDeletionLag, in minutes, for a namespace

Usage

```bash

$ pulsar-admin namespaces get-offload-deletion-lag tenant/namespace

```

### `set-offload-deletion-lag`
Set offloadDeletionLag for a namespace

Usage

```bash

$ pulsar-admin namespaces set-offload-deletion-lag tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-l`, `--lag`|Duration to wait after offloading a ledger segment, before deleting the copy of that segment from cluster local storage. (eg: 10m, 5h, 3d, 2w).|-1|

### `clear-offload-deletion-lag`
Clear offloadDeletionLag for a namespace

Usage

```bash

$ pulsar-admin namespaces clear-offload-deletion-lag tenant/namespace

```

### `get-schema-autoupdate-strategy`
Get the schema auto-update strategy for a namespace

Usage

```bash

$ pulsar-admin namespaces get-schema-autoupdate-strategy tenant/namespace

```

### `set-schema-autoupdate-strategy`
Set the schema auto-update strategy for a namespace

Usage

```bash

$ pulsar-admin namespaces set-schema-autoupdate-strategy tenant/namespace options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-c`, `--compatibility`|Compatibility level required for new schemas created via a Producer. Possible values (Full, Backward, Forward, None).|Full|
|`-d`, `--disabled`|Disable automatic schema updates.|false|


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
* `brokers`
* `broker`

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

### `brokers`
List all brokers with namespace-isolation policies attached to it. This operation requires Pulsar super-user privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy brokers cluster-name

```

### `broker`
Get broker with namespace-isolation policies attached to it. This operation requires Pulsar super-user privileges.

Usage

```bash

$ pulsar-admin ns-isolation-policy broker cluster-name options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--broker`|Broker name to get namespace-isolation policies attached to it||


## `sinks`

An interface for managing Pulsar IO sinks (egress data from Pulsar)

Usage

```bash

$ pulsar-admin sinks subcommand

```

Subcommands
* `create`
* `update`
* `delete`
* `list`
* `get`
* `status`
* `stop`
* `start`
* `restart`
* `localrun`
* `available-sinks`


### `create`
Submit a Pulsar IO sink connector to run in a Pulsar cluster

Usage

```bash

$ pulsar-admin sinks create options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The sink's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|The sink’s parallelism factor (i.e. the number of sink instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|User defined configs key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs (supported for java fun only)||
|`--tenant`|The sink’s tenant||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--timeout-ms`|The message timeout in milliseconds||
|`--retain-ordering`|Sink consumes and sinks messages in order||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||


### `update`
Update a Pulsar IO sink connector

Usage

```bash

$ pulsar-admin sinks update options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The sink's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|The sink’s parallelism factor (i.e. the number of sink instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|User defined configs key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs (supported for java fun only)||
|`--tenant`|The sink’s tenant||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--retain-ordering`|Sink consumes and sinks messages in order||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--timeout-ms`|The message timeout in milliseconds||


### `delete`
Stops a Pulsar IO sink connector

Usage

```bash

$ pulsar-admin sinks delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `list`
List all running Pulsar IO sink connectors

Usage

```bash

$ pulsar-admin sinks list options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `get`
Gets the information about a Pulsar IO sink connector

Usage

```bash

$ pulsar-admin sinks get options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `status`
Check the current status of a Pulsar Sink

Usage

```bash

$ pulsar-admin sinks status options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The sink instanceId (Get-status of all instances if instance-id is not provided)||
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `stop`
Stops sink instance

Usage

```bash

$ pulsar-admin sinks stop options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The sink instanceId (stop all instances if instance-id is not provided)||
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `start`
Starts sink instance

Usage

```bash

$ pulsar-admin sinks start options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The sink instanceId (start all instances if instance-id is not provided)||
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `restart`
Restart sink instance

Usage

```bash

$ pulsar-admin sinks restart options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The sink instanceId (restart all instances if instance-id is not provided)||
|`--name`|The sink's name||
|`--namespace`|The sink's namespace||
|`--tenant`|The sink's tenant||


### `localrun`
Run a Pulsar IO sink connector locally (rather than deploying it to the Pulsar cluster)

Usage

```bash

$ pulsar-admin sinks localrun options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--broker-service-url`|The URL for the Pulsar broker||
|`--classname`|The sink's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--custom-serde-inputs`|The map of input topics to SerDe class names (as a JSON string)||
|`--custom-schema-inputs`|The map of input topics to Schema types or class names (as a JSON string)||
|`--disk`|The disk (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--inputs`|The sink’s input topic(s) (multiple topics can be specified as a comma-separated list)||
|`--archive`|Path to the archive file for the sink. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The sink’s name||
|`--namespace`|The sink’s namespace||
|`--parallelism`|The sink’s parallelism factor (i.e. the number of sink instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the sink. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per sink instance (applicable only to the Docker runtime)||
|`--sink-config`|User defined configs key/values||
|`--sink-config-file`|The path to a YAML config file specifying the sink’s configuration||
|`--sink-type`|The built-in sinks's connector provider. The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--topics-pattern`|TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs (supported for java fun only)||
|`--tenant`|The sink’s tenant||
|`--auto-ack`|Whether or not the framework will automatically acknowledge messages||
|`--timeout-ms`|The message timeout in milliseconds||
|`--client-auth-params`|Client authentication param||
|`--client-auth-plugin`|Client authentication plugin using which function-process can connect to broker||
|`--hostname-verification-enabled`|Enable hostname verification|false|
|`--retain-ordering`|Sink consumes and sinks messages in order||
|`--subs-name`|Pulsar source subscription name if user wants a specific subscription-name for input-topic consumer||
|`--tls-allow-insecure`|Allow insecure tls connection|false|
|`--tls-trust-cert-path`|The tls trust cert file path||
|`--use-tls`|Use tls connection|false|


### `available-sinks`
Get the list of Pulsar IO connector sinks supported by Pulsar cluster

Usage

```bash

$ pulsar-admin sinks available-sinks

```

## `sources`
An interface for managing Pulsar IO sources (ingress data into Pulsar)

Usage

```bash

$ pulsar-admin sources subcommand

```

Subcommands
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


### `create`
Submit a Pulsar IO source connector to run in a Pulsar cluster

Usage

```bash

$ pulsar-admin sources create options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||


### `update`
Update a already submitted Pulsar IO source connector

Usage

```bash

$ pulsar-admin sources update options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||


### `delete`
Stops a Pulsar IO source connector

Usage

```bash

$ pulsar-admin sources delete options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `get`
Gets the information about a Pulsar IO source connector

Usage

```bash

$ pulsar-admin sources get options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `status`
Check the current status of a Pulsar Source

Usage

```bash

$ pulsar-admin sources status options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The source instanceId (Get-status of all instances if instance-id is not provided)||
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `list`
List all running Pulsar IO source connectors

Usage

```bash

$ pulsar-admin sources list options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `stop`
Stop source instance

Usage

```bash

$ pulsar-admin sources stop options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The source instanceId (stop all instances if instance-id is not provided)||
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `start`
Start source instance

Usage

```bash

$ pulsar-admin sources start options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The source instanceId (start all instances if instance-id is not provided)||
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `restart`
Restart source instance

Usage

```bash

$ pulsar-admin sources restart options

```

Options

|Flag|Description|Default|
|---|---|---|
|`--instance-id`|The source instanceId (restart all instances if instance-id is not provided)||
|`--name`|The source's name||
|`--namespace`|The source's namespace||
|`--tenant`|The source's tenant||


### `localrun`
Run a Pulsar IO source connector locally (rather than deploying it to the Pulsar cluster)

Usage

```bash

$ pulsar-admin sources localrun options

```

Options

|Flag|Description|Default|
|----|---|---|
|`--classname`|The source's class name if archive is file-url-path (file://)||
|`--cpu`|The CPU (in cores) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--deserialization-classname`|The SerDe classname for the source||
|`--destination-topic-name`|The Pulsar topic to which data is sent||
|`--disk`|The disk (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--archive`|The path to the NAR archive for the Source. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.||
|`--name`|The source’s name||
|`--namespace`|The source’s namespace||
|`--parallelism`|The source’s parallelism factor (i.e. the number of source instances to run).||
|`--processing-guarantees`|The processing guarantees (aka delivery semantics) applied to the source. Available values: ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE.||
|`--ram`|The RAM (in bytes) that needs to be allocated per source instance (applicable only to the Docker runtime)||
|`--schema-type`|The schema type (either a builtin schema like 'avro', 'json', etc, or custom Schema class name to be used to encode messages emitted from the source||
|`--source-type`|One of the built-in source's connector provider. The `source-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.||
|`--source-config`|Source config key/values||
|`--source-config-file`|The path to a YAML config file specifying the source’s configuration||
|`--tenant`|The source’s tenant||
|`--broker-service-url`|The URL for the Pulsar broker||
|`--client-auth-params`|Client authentication param||
|`--client-auth-plugin`|Client authentication plugin using which function-process can connect to broker||
|`--hostname-verification-enabled`|Enable hostname verification|false|
|`--tls-allow-insecure`|Allow insecure tls connection|false|
|`--tls-trust-cert-path`|The tls trust cert file path||
|`--use-tls`|Use tls connection||


### `available-sources`
Get the list of Pulsar IO connector sources supported by Pulsar cluster

Usage

```bash

$ pulsar-admin sources available-sources

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
* `create`
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

### `create`
Creates a non-partitioned topic. A non-partitioned topic must explicitly be created by the user if allowAutoTopicCreation or createIfMissing is disabled.

Usage

```bash

$ pulsar-admin topics create {persistent|non-persistent}://tenant/namespace/topic

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
* `extract`


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

### `extract`
Provide the schema definition for a topic via Java class name contained in a JAR file

Usage

```bash

$ pulsar-admin schemas extract persistent://tenant/namespace/topic options

```

Options

|Flag|Description|Default|
|----|---|---|
|`-c`, `--classname`|The Java class name||
|`-j`, `--jar`|A path to the JAR file which contains the above Java class||
|`-t`, `--type`|The type of the schema (avro or json)||


