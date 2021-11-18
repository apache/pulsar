---
id: functions-cli
title: Pulsar Functions command line tool
sidebar_label: "Reference: CLI"
---

The following tables list Pulsar Functions command-line tools. You can learn Pulsar Functions modes, commands, and parameters.

## localrun

Run Pulsar Functions locally, rather than deploying it to the Pulsar cluster.

Name | Description | Default
---|---|---
auto-ack | Whether or not the framework acknowledges messages automatically. | true |
broker-service-url | The URL for the Pulsar broker. | |
classname | The class name of a Pulsar Function.| |
client-auth-params | Client authentication parameter. | |
client-auth-plugin | Client authentication plugin using which function-process can connect to broker. |  |
CPU | The CPU in cores that need to be allocated per function instance (applicable only to docker runtime).| |
custom-schema-inputs | The map of input topics to Schema class names (as a JSON string). | |
custom-serde-inputs | The map of input topics to SerDe class names (as a JSON string). | |
dead-letter-topic | The topic where all messages that were not processed successfully are sent. This parameter is not supported in Python Functions.  | |
disk | The disk in bytes that need to be allocated per function instance (applicable only to docker runtime). | |
fqfn | The Fully Qualified Function Name (FQFN) for the function. |  |
function-config-file | The path to a YAML config file specifying the configuration of a Pulsar Function. |  |
go | Path to the main Go executable binary for the function (if the function is written in Go). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
hostname-verification-enabled | Enable hostname verification. | false
inputs | The input topic or topics of a Pulsar Function (multiple topics can be specified as a comma-separated list). | |
jar | Path to the jar file for the function (if the function is written in Java). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
instance-id-offset | Start the instanceIds from this offset. | 0
log-topic | The topic to which the logs  a Pulsar Function are produced. |  |
max-message-retries | How many times should we try to process a message before giving up. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
output | The output topic of a Pulsar Function (If none is specified, no output is written). |  |
output-serde-classname | The SerDe class to be used for messages output by the function. |  |
parallelism | The parallelism factor of  a Pulsar Function (i.e. the number of function instances to run). |  |
processing-guarantees | The processing guarantees (delivery semantics) applied to the function. Available values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]. | ATLEAST_ONCE
py | Path to the main Python file/Python Wheel file for the function (if the function is written in Python). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
ram | The ram in bytes that need to be allocated per function instance (applicable only to process/docker runtime). |  |
retain-ordering | Function consumes and processes messages in order. | |
schema-type | The builtin schema type or custom schema class name to be used for messages output by the function. | |
sliding-interval-count | The number of messages after which the window slides. |  |
sliding-interval-duration-ms | The time duration after which the window slides. |  |
subs-name | Pulsar source subscription name if user wants a specific subscription-name for the input-topic consumer. |  |
tenant | The tenant of a Pulsar Function. |  |
timeout-ms | The message timeout in milliseconds. |  |
tls-allow-insecure | Allow insecure tls connection. | false
tls-trust-cert-path | tls trust cert file path. |  |
topics-pattern | The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (only supported in Java Function). |  |
use-tls | Use tls connection. | false
user-config | User-defined config key/values. |  |
window-length-count | The number of messages per window. |  |
window-length-duration-ms | The time duration of the window in milliseconds. | |


## create

Create and deploy a Pulsar Function in cluster mode.

Name | Description | Default
---|---|---
auto-ack | Whether or not the framework acknowledges messages automatically. | true |
classname | The class name of a Pulsar Function. |  |
CPU | The CPU in cores that need to be allocated per function instance (applicable only to docker runtime).| |
custom-runtime-options | A string that encodes options to customize the runtime, see docs for configured runtime for details | |
custom-schema-inputs | The map of input topics to Schema class names (as a JSON string). | |
custom-serde-inputs | The map of input topics to SerDe class names (as a JSON string). | |
dead-letter-topic | The topic where all messages that were not processed successfully are sent. This parameter is not supported in Python Functions. | |
disk | The disk in bytes that need to be allocated per function instance (applicable only to docker runtime). | |
fqfn | The Fully Qualified Function Name (FQFN) for the function. |  |
function-config-file | The path to a YAML config file specifying the configuration of a Pulsar Function. |  |
go | Path to the main Go executable binary for the function (if the function is written in Go). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
inputs | The input topic or topics of a Pulsar Function (multiple topics can be specified as a comma-separated list). | |
jar | Path to the jar file for the function (if the function is written in Java). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
log-topic | The topic to which the logs of a Pulsar Function are produced. |  |
max-message-retries | How many times should we try to process a message before giving up. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
output | The output topic of a Pulsar Function (If none is specified, no output is written). |  |
output-serde-classname | The SerDe class to be used for messages output by the function. |  |
parallelism | The parallelism factor of a Pulsar Function (i.e. the number of function instances to run). |  |
processing-guarantees | The processing guarantees (delivery semantics) applied to the function. Available values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]. | ATLEAST_ONCE
py | Path to the main Python file/Python Wheel file for the function (if the function is written in Python). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
ram | The ram in bytes that need to be allocated per function instance (applicable only to process/docker runtime). |  |
retain-ordering | Function consumes and processes messages in order. |  |
schema-type | The builtin schema type or custom schema class name to be used for messages output by the function. | |
sliding-interval-count | The number of messages after which the window slides. |  |
sliding-interval-duration-ms | The time duration after which the window slides. |  |
subs-name | Pulsar source subscription name if user wants a specific subscription-name for the input-topic consumer. |  |
tenant | The tenant of a Pulsar Function. |  |
timeout-ms | The message timeout in milliseconds. |  |
topics-pattern | The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (only supported in Java Function). |  |
user-config | User-defined config key/values. |  |
window-length-count | The number of messages per window. |  |
window-length-duration-ms | The time duration of the window in milliseconds. | |

## delete

Delete a Pulsar Function that is running on a Pulsar cluster.

Name | Description | Default
---|---|---
fqfn | The Fully Qualified Function Name (FQFN) for the function. | |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
tenant | The tenant of a Pulsar Function. |  |

## update

Update a Pulsar Function that has been deployed to a Pulsar cluster.

Name | Description | Default
---|---|---
auto-ack | Whether or not the framework acknowledges messages automatically. | true |
classname | The class name of a Pulsar Function. | |
CPU | The CPU in cores that need to be allocated per function instance (applicable only to docker runtime). | |
custom-runtime-options | A string that encodes options to customize the runtime, see docs for configured runtime for details | |
custom-schema-inputs | The map of input topics to Schema class names (as a JSON string). | |
custom-serde-inputs | The map of input topics to SerDe class names (as a JSON string). | |
dead-letter-topic | The topic where all messages that were not processed successfully are sent. This parameter is not supported in Python Functions. | |
disk | The disk in bytes that need to be allocated per function instance (applicable only to docker runtime). | |
fqfn | The Fully Qualified Function Name (FQFN) for the function. |  |
function-config-file | The path to a YAML config file specifying the configuration of a Pulsar Function. |  |
go | Path to the main Go executable binary for the function (if the function is written in Go). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
inputs | The input topic or topics of a Pulsar Function (multiple topics can be specified as a comma-separated list). | |
jar | Path to the jar file for the function (if the function is written in Java). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
log-topic | The topic to which the logs of a Pulsar Function are produced. |  |
max-message-retries | How many times should we try to process a message before giving up. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
output | The output topic of a Pulsar Function (If none is specified, no output is written). |  |
output-serde-classname | The SerDe class to be used for messages output by the function. |  |
parallelism | The parallelism factor of a Pulsar Function (i.e. the number of function instances to run). |  |
processing-guarantees | The processing guarantees (delivery semantics) applied to the function. Available values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]. | ATLEAST_ONCE
py | Path to the main Python file/Python Wheel file for the function (if the function is written in Python). It also supports URL path [http/https/file (file protocol assumes that file already exists on worker host)/function (package URL from packages management service)] from which worker can download the package. |  |
ram | The ram in bytes that need to be allocated per function instance (applicable only to process/docker runtime). |  |
retain-ordering | Function consumes and processes messages in order. |  |
schema-type | The builtin schema type or custom schema class name to be used for messages output by the function. | |
sliding-interval-count | The number of messages after which the window slides. |  |
sliding-interval-duration-ms | The time duration after which the window slides. |  |
subs-name | Pulsar source subscription name if user wants a specific subscription-name for the input-topic consumer. |  |
tenant | The tenant of a Pulsar Function. |  |
timeout-ms | The message timeout in milliseconds. |  |
topics-pattern | The topic pattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (only supported in Java Function). |  |
update-auth-data | Whether or not to update the auth data. | false
user-config | User-defined config key/values. |  |
window-length-count | The number of messages per window. |  |
window-length-duration-ms | The time duration of the window in milliseconds. | |

## get

Fetch information about a Pulsar Function.

Name | Description | Default
---|---|---
fqfn | The Fully Qualified Function Name (FQFN) for the function. | |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
tenant | The tenant of a Pulsar Function. |  |

## restart

Restart function instance.

Name | Description | Default
---|---|---
fqfn | The Fully Qualified Function Name (FQFN) for the function. | |
instance-id | The function instanceId (restart all instances if instance-id is not provided. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
tenant | The tenant of a Pulsar Function. |  |

## stop

Stops function instance.

Name | Description | Default
---|---|---
fqfn | The Fully Qualified Function Name (FQFN) for the function. | |
instance-id | The function instanceId (restart all instances if instance-id is not provided. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
tenant | The tenant of a Pulsar Function. |  |

## start

Starts a stopped function instance.

Name | Description | Default
---|---|---
fqfn | The Fully Qualified Function Name (FQFN) for the function. | |
instance-id | The function instanceId (restart all instances if instance-id is not provided. |  |
name | The name of a Pulsar Function. |  |
namespace | The namespace of a Pulsar Function. |  |
tenant | The tenant of a Pulsar Function. |  |
