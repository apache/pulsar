# Pulsar Functions Python Runtime

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