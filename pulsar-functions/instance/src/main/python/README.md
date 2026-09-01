# Pulsar Functions Python Runtime

### pulsar-client-python requirements

The runtime requires **pulsar-client-python 3.3.0 or newer**. `Client.subscribe()` grew its
`dead_letter_policy` parameter in 3.3.0 (as did `ConsumerDeadLetterPolicy`, in the same release), and
the runtime passes the keyword on every subscription — as `None` when the function configures no
`maxMessageRetries` or `deadLetterTopic`, which `Client.subscribe()` treats as a no-op. Every other
argument the runtime passes is present in 3.2.0, so this is the only thing setting the floor.

Everything Pulsar ships already satisfies this: the `pulsar-client-python` version in
[`gradle/libs.versions.toml`](../../../../../gradle/libs.versions.toml) is what the
[Docker images](../../../../../docker/pulsar/Dockerfile) install and what CI runs the instance tests
against. The floor is only visible on a self-managed worker, where the process runtime launches the
host's `python3` (see `RuntimeUtils`) and the installed client is the operator's. For a
zip-packaged function, pin it in the function's own `requirements.txt`, which
`python_instance_main.py` pip-installs before the instance starts:

```
pulsar-client>=3.3.0
```

### Producer configuration

Both producers the runtime creates — the sink (output topic) producer in `python_instance.py` and the
producers behind `context.publish()` in `contextimpl.py` — are configured from the `producerSpec` of
the function's sink, which reaches the instance inside the `FunctionDetails` protobuf. The
translation lives in `util.producer_config_from_function_details()`.

| `ProducerSpec` field | `Client.create_producer()` keyword |
|---|---|
| `maxPendingMessages` | `max_pending_messages` |
| `maxPendingMessagesAcrossPartitions` | `max_pending_messages_across_partitions` |
| `batchBuilder` | `batching_type` |
| `compressionType` | `compression_type` (sink producer only; `context.publish()` takes a per-call value) |
| `cryptoSpec` | `crypto_key_reader`, `encryption_key` (sink producer only) |
| `batchingSpec.enabled` | `batching_enabled` |
| `batchingSpec.batchingMaxPublishDelayMs` | `batching_max_publish_delay_ms` |
| `batchingSpec.batchingMaxMessages` | `batching_max_messages` |
| `batchingSpec.batchingMaxBytes` | `batching_max_allowed_size_in_bytes` |
| `batchingSpec.batchBuilder` | `batching_type` (takes precedence over `ProducerSpec.batchBuilder`) |

These are the same settings a user configures through `producerConfig` on the function config, which
PIP-401 extended with `batchingConfig`.

A few rules keep the behaviour aligned with the Java runtime
(`ProducerBuilderFactory` and `BatchingUtils`):

- **A field that is unset or non-positive in the spec is left out**, so the Python client's own
  default applies rather than an explicit zero.
- **A sink with no `producerSpec`, or a `producerSpec` with no `batchingSpec`, gets batching enabled
  with a 10ms maximum publish delay.** This is the long-standing default and must not change: it is
  what functions written before batching became configurable already run with.
- **`batchingSpec.batchBuilder` wins over `ProducerSpec.batchBuilder`**, because the Java runtime
  applies them in that order.

`batchingSpec.roundRobinRouterBatchingPartitionSwitchFrequency` has no equivalent in the Python
client and is ignored. `block_if_queue_full` is fixed at `True`, matching the Java runtime, which
also hardcodes `blockIfQueueFull(true)` and exposes no configuration for it.

### Updating Protobuf and gRPC generated stubs

When using generated Protobuf and gRPC stubs (`*_pb2.py`, `*_pb2_gprc.py`), the generated code should be 
updated when the grpcio and protobuf Python packages are updated. This is due to the fact that generated 
Protobuf and gRPC stubs are not necessarily compatible across different versions of these packages at runtime. 
The compatibility policy of Protobuf is documented in 
[Protobuf's "Cross-Version Runtime Guarantee"](https://protobuf.dev/support/cross-version-runtime-guarantee/),
which states that cross-version runtime support isn't guaranteed. gRPC follows a similar policy.

In Pulsar's [Docker image](../../../../../docker/pulsar/Dockerfile), the `grpcio` and `protobuf` packages are
pinned to specific versions. Whenever these versions are updated, the `PYTHON_GRPCIO_VERSION` 
in [src/update_python_protobuf_stubs.sh](../../../../../src/update_python_protobuf_stubs.sh) should also be updated
and the generated stubs should be regenerated with this script to ensure compatibility.

To update the generated stubs, run the following command in the project root directory:

```bash
# run this command from the project root directory
src/update_python_protobuf_stubs.sh
```

Alternatively, you can run this command to install the required tools in a docker container and update the stubs:

```bash
# run this command from the project root directory
src/update_python_protobuf_stubs_with_docker.sh
```

When the script is run, it will also print such information to the console:

```
libprotoc library included in grpcio-tools will be used:
libprotoc 31.0
The compatible matching protobuf package version in Python is prefixed with '6.'
Ensure that you are using a compatible version of the protobuf package such as 6.31.0 (or a matching patch version).
```

When pinning the `protobuf` package in your Python project follow this guidance to ensure compatibility of the generated stubs with the `protobuf` package version.