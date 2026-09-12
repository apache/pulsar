# PIP-43: producer send message with different schema

* **Status**: Adopted, Release 2.5.0
* **Author**: [Yi Tang](https://github.com/yittg)
* **Master Issue**: [apache/pulsar#5141](https://github.com/apache/pulsar/issues/5141)
* **Mailing List discussion**: [lists.apache.org/...](https://lists.apache.org/thread.html/284c98f3d5c3e5fe0f43964b3bbeb665316e69a4f9e97bda78a47026@%3Cdev.pulsar.apache.org%3E)

## Motivation

For now, Pulsar producer can only produce messages of one type of schema which is determined by the user when it is created, or by fetching the latest version of schema from the registry if `AUTO_PRODUCE_BYTES` type is specified. Schema, however, can be updated by the external system after producer started, which would lead to inconsistency between message payload and schema version metadata. Also, some scenarios like replicating from Kafka require a single producer for replicating messages of different schemas from one Kafka partition to one Pulsar partition to guarantee the order and no duplicates.

Here proposing that messages can indicate the associated schema by itself with two parts of changes to make it clear.

## Changes:Part-1

For the part-1, here propose that producer supports to new message specified with schema, particularly, of same POJO type.

### Interfaces

For the `Producer<T>` interface, here propose a new method for `newMessage` to new a message builder with a specified schema with the following signature:

```
TypedMessageBuilder<T> newMessage(Schema<T> schema);
```

where the parameterized type `T` is required to be same with the producer.

For `AutoProduceBytesSchema` especially, the user SHOULD new message with actual schema wrapped by auto produce bytes schema. A static method MAY be provided by the `Schema` interface with the following signature:

```
static Schema<byte[]> AUTO_PRODUCE_BYTES(Schema<?> schema);
```

### Wire protocols

To guarantee scenarios that send a message with a brand new schema, we also propose a new Command to get schema version, or create one if NOT present.

```
message CommandGetOrCreateSchema {
    required uint64 request_id = 1;
    required string topic      = 2;
    required Schema schema     = 3;
}

message CommandGetOrCreateSchemaResponse {
    required uint64 request_id      = 1;
    optional ServerError error_code = 2;
    optional string error_message   = 3;

    optional bytes schema_version   = 4;
}
```

### Implementation

#### Client
The current `Schema schema` field of `Producer` would be used as default schema when a producer sends messages without specifying schema explicitly, with which the default schema would be associated.

Producer SHOULD maintain a local map from schema to schema version and check the version of the associated schema of a message before sending. If the schema can not be found, producer SHOULD try to register this schema to the registry and get the version of it, then insert the pair to the local map. Hash of `Schema` same with registry CAN be used as the key of the map.

Producer SHOULD also attach the actual schema version to the message metadata as it is.

For batch messages share single same metadata, only one schema version is allowed, so before adding one message into a batch container, producer SHOULD check the schema version of this batch, and flush the batch if associated with different schema from the message and initialize another batch for it.

To be seamless for the producer does not require this feature, an option CAN be added to producer builder to switch this feature and disabled by default, also automatically enabled if it touches this feature, e.g. new a message with the method introduced in this proposal. When this feature is NOT enabled, the action of producer SHOULD keep as it is.

#### Broker
The server SHOULD handle the new schema command, put the schema to the registry or just respond the corresponding existing version, which SHOULD be built on top of compatibility check. The registry backend has already implemented this interface.

#### Functions
Functions MAY inherit the feature gate option and expose it to configuration, same for Sources.

## Changes:Part-2

For the part-2, here propose to allow the producer to new message with different POJO type of schema. To be noted that, once one producer can send different POJOs, the parameterized type of message involved in methods of `Producer` SHOULD be changed in some way. The interceptor mechanism would also be affected.

### Interfaces

For the `Producer<T>` interface, here propose to enhance the method proposed in part-1 to accept arbitrary inner type with the following signature:

```
<V> TypedMessageBuilder<V> newMessage(Schema<V> schema);
```
where the parameterized type `T` and `V` is NOT required to be same.

For `ProducerInterceptor<T>` interface, provide a method to indicate whether the message is supported by the interceptor instance:
```
default bool eligible(Message message) {
    return true;
}
```
which is essential especially when the producer can send different type of messages, notes that the message parameter is `Message` raw type, not required to have the same parameterized type with the interceptor.

### Implementations

#### Client
The only thing that needs to be pointed out is when the parameterized type of producer and schema/message conflict, the message parameter is allowed to declare different parameterized type.

For `ProducerInterceptors`, check if eligible before invoking each interceptor.
