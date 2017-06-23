
## Protocol Buffer code generation

Pulsar uses protocol buffer messages for the client/broker wire protocol. 

The protocol definition is located at `pulsar-common/src/main/proto/PulsarApi.proto`. When making a change to the `PulsarApi.proto` definition, we have to regenerate the `PulsarApi.*` files and include them in the same commit.

### For Broker and Java Client:

The pre-generated Java code is at `pulsar-common/src/main/java/org/apache/pulsar/common/api/proto/PulsarApi.java`. 

We are currently using a modified version of the Google Protocol Buffer code generator, to generate code that can serialize/deserialize messages with no memory allocations (caching already instantiated objects) and also to be able to directly use Netty pooled ByteBuf with direct memory.

To re-generate the `PulsarApi.java` code you need to apply a patch to the protobuf generator. Patch is found in `protobuf.patch`.

### For C++ Client:

The pre-generated C++ code is at `pulsar-client-cpp/lib/PulsarApi.pb.cc` and `pulsar-client-cpp/lib/PulsarApi.pb.h`.

### Commands for creating the pre-generated files

```shell
export PULSAR_HOME=<Path where you cloned the pulsar repo>

cd $HOME
git clone https://github.com/google/protobuf.git

### For C++ ###
cd ${HOME}/protobuf
git checkout v2.6.0

### Compile protobuf
autoreconf --install
./configure
make

### Re-generate PulsarApi
cd ${PULSAR_HOME}/pulsar-client-cpp/
export PROTOC=${HOME}/protobuf/src/protoc 
./generate_protobuf.sh

### For Java ###
cd ${HOME}/protobuf
git checkout v2.4.1

### Apply patch
patch -p1 < ${PULSAR_HOME}/protobuf/protobuf.patch

### Compile protobuf
autoreconf --install
./configure
make

### Re-generate PulsarApi
cd ${PULSAR_HOME}/pulsar-common/
export PROTOC=${HOME}/protobuf/src/protoc 
./generate_protobuf.sh
```
