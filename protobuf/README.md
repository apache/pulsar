
## Protocol Buffer code generation

Pulsar uses protocol buffer messages for the client/broker wire protocol. 

The protocol definition is located at `pulsar-common/src/main/proto/PulsarApi.proto` and the pre-generated Java code is at `pulsar-common/src/main/java/com/yahoo/pulsar/common/api/proto/PulsarApi.java`. 

When making a change to the `PulsarApi.proto` definition, we have regenerate the `PulsarApi.java` and include that in the same commit.

We are currently using a modified version of the Google Protocol Buffer code generator, to generate code that can serialize/deserialize messages with no memory allocations (caching already instantiated objects) and also to be able to directly use Netty pooled ByteBuf with direct memory.

To re-generate the `PulsarApi.java` code you need to apply a patch to the protobuf generator. Patch is found at `protobuf.patch`.

```shell
git clone https://github.com/google/protobuf.git
cd protobuf
git checkout v2.4.1

# Apply patch
patch -p1 < ../pulsar-path/protobuf/protobuf.patch

# Compile protobuf
./configure
make

# This would leave the binary in src/protoc


# Re-generate PulsarApi
cd pulsar-path/pulsar-common
PROTOC=~/protobuf/src/protoc ./generate_protobuf.sh

```
