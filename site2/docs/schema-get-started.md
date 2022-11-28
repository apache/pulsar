---
id: schema-get-started
title: Get started
sidebar_label: "Get started"
---


````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


This hands-on tutorial provides instructions and examples on how to construct schemas. For the instructions on administrative tasks, see [Manage schema](admin-api-schemas.md).

## Construct a schema

- [Construct a bytes schema](#bytes)
- [Construct a string schema](#string)
- [Construct a key/value schema](#keyvalue)
- [Construct an Avro schema](#avro)
- [Construct a JSON schema](#json)
- [Construct a Protobuf schema](#protobuf)
- [Construct a ProtobufNative schema](#protobufnative)
- [Construct a native Avro schema](#nativeavro)
- [Construct an AUTO_PRODUCE schema](#auto_produce)
- [Construct an AUTO_CONSUME schema](#auto_consume)

### `bytes`

This example demonstrates how to construct a [bytes schema](schema-understand.md#primitive-type) using language-specific clients and use it to produce and consume messages.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>

<TabItem value="Java">

```java
       .topic("my-topic")
       .create();
Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
       .topic("my-topic")
       .subscriptionName("my-sub")
       .subscribe();

producer.newMessage().value("message".getBytes()).send();

Message<byte[]> message = consumer.receive(5, TimeUnit.SECONDS);
```

</TabItem>
<TabItem value="C++">

```cpp
SchemaInfo schemaInfo = SchemaInfo(SchemaType::BYTES, "Bytes", "");
Producer producer;
client.createProducer("topic-bytes", ProducerConfiguration().setSchema(schemaInfo), producer);
std::array<char, 1024> buffer;
producer.send(MessageBuilder().setContent(buffer.data(), buffer.size()).build());
Consumer consumer;
res = client.subscribe("topic-bytes", "my-sub", ConsumerConfiguration().setSchema(schemaInfo), consumer);
Message msg;
consumer.receive(msg, 3000);
```

</TabItem>
<TabItem value="Python">

```python
producer = client.create_producer(
'bytes-schema-topic',
schema=BytesSchema())
producer.send(b"Hello")

consumer = client.subscribe(
'bytes-schema-topic',
		'sub',
		schema=BytesSchema())
msg = consumer.receive()
data = msg.value()
```

</TabItem>
<TabItem value="Go">

```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
  Topic:  "my-topic",
  Schema: pulsar.NewBytesSchema(nil),
})
id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
  Value: []byte("message"),
})

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
  Topic:            "my-topic",
  Schema:           pulsar.NewBytesSchema(nil),
  SubscriptionName: "my-sub",
  Type:             pulsar.Exclusive,
})
```

</TabItem>
</Tabs>
````

### `string`

This example demonstrates how to construct a [string schema](schema-understand.md#primitive-type) using language-specific clients and use it to produce and consume messages.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>

<TabItem value="Java">

   ```java
   Producer<String> producer = client.newProducer(Schema.STRING).create();
   producer.newMessage().value("Hello Pulsar!").send();

   Consumer<String> consumer = client.newConsumer(Schema.STRING).subscribe();
   Message<String> message = consumer.receive();
   ```

</TabItem>
<TabItem value="C++">

```cpp
SchemaInfo schemaInfo = SchemaInfo(SchemaType::STRING, "String", "");
Producer producer;
client.createProducer("topic-string", ProducerConfiguration().setSchema(schemaInfo), producer);
producer.send(MessageBuilder().setContent("message").build());

Consumer consumer;
client.subscribe("topic-string", "my-sub", ConsumerConfiguration().setSchema(schemaInfo), consumer);
Message msg;
consumer.receive(msg, 3000);
```

</TabItem>
<TabItem value="Python">

```python
producer = client.create_producer(
                'string-schema-topic',
                schema=StringSchema())
producer.send("Hello")

consumer = client.subscribe(
		'string-schema-topic',
		'sub',
		schema=StringSchema())
msg = consumer.receive()
str = msg.value()
```

</TabItem>
<TabItem value="Go">

```go
producer, err := client.CreateProducer(pulsar.ProducerOptions{
  Topic:  "my-topic",
  Schema: pulsar.NewStringSchema(nil),
})
id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
  Value: "message",
})

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
  Topic:            "my-topic",
  Schema:           pulsar.NewStringSchema(nil),
  SubscriptionName: "my-sub",
  Type:             pulsar.Exclusive,
})
msg, err := consumer.Receive(context.Background())
```

</TabItem>
</Tabs>
````

### `key/value`

This example shows how to construct a [key/value schema](schema-understand.md#keyvalue-schema) using language-specific clients and use it to produce and consume messages.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"}]}>

<TabItem value="Java">

1. Construct a key/value schema with `INLINE` encoding type.

    ```java
    Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
        Schema.INT32,
        Schema.STRING,
        KeyValueEncodingType.INLINE
    );
    ```

   Alternatively, construct a key/value schema with `SEPARATED` encoding type.

   ```java
   Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
       Schema.INT32,
       Schema.STRING,
       KeyValueEncodingType.SEPARATED
   );
   ```

2. Produce messages using a key/value schema.

   ```java
   Producer<KeyValue<Integer, String>> producer = client.newProducer(kvSchema)
       .topic(topicName)
       .create();

   final int key = 100;
   final String value = "value-100";

   // send the key/value message
   producer.newMessage()
       .value(new KeyValue(key, value))
       .send();
   ```

3. Consume messages using a key/value schema.

   ```java
   Consumer<KeyValue<Integer, String>> consumer = client.newConsumer(kvSchema)
       ...
       .topic(topicName)
       .subscriptionName(subscriptionName).subscribe();

   // receive key/value pair
   Message<KeyValue<Integer, String>> msg = consumer.receive();
   KeyValue<Integer, String> kv = msg.getValue();
   ```

</TabItem>
<TabItem value="C++">

1. Construct a key/value schema with `INLINE` encoding type.

   ```cpp
   //Prepare keyValue schema
   std::string jsonSchema =
   R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
   SchemaInfo keySchema(JSON, "key-json", jsonSchema);
   SchemaInfo valueSchema(JSON, "value-json", jsonSchema);
   SchemaInfo keyValueSchema(keySchema, valueSchema, KeyValueEncodingType::INLINE);
   ```

2. Produce messages using a key/value schema.

   ```cpp
   //Create Producer
   Producer producer;
   client.createProducer("my-topic", ProducerConfiguration().setSchema(keyValueSchema), producer);

   //Prepare message
   std::string jsonData = "{\"re\":2.1,\"im\":1.23}";
   KeyValue keyValue(std::move(jsonData), std::move(jsonData));
   Message msg = MessageBuilder().setContent(keyValue).setProperty("x", "1").build();
   //Send message
   producer.send(msg);
   ```

3. Consume messages using a key/value schema.

   ```cpp
   //Create Consumer
   Consumer consumer;
   client.subscribe("my-topic", "my-sub", ConsumerConfiguration().setSchema(keyValueSchema), consumer);

   //Receive message
   Message message;
   consumer.receive(message);
   ```

</TabItem>
</Tabs>
````

### `struct`

This example shows how to construct a [struct schema](schema-understand.md#struct-schema) and use it to produce and consume messages using different methods.

````mdx-code-block
<Tabs 
  defaultValue="static"
  values={[{"label":"static","value":"static"},{"label":"generic","value":"generic"},{"label":"SchemaDefinition","value":"SchemaDefinition"}]}>

<TabItem value="static">

You can predefine the `struct` schema, which can be a POJO in Java, a `struct` in Go, or classes generated by Avro or Protobuf tools. 

**Example** 

Pulsar gets the schema definition from the predefined `struct` using an Avro library. The schema definition is the schema data stored as a part of the `SchemaInfo`.

1. Create the _User_ class to define the messages sent to Pulsar topics.

   ```java
   public static class User {
       public String name;
       public int age;
       public User(String name, int age) {
 	this.name = name;
	this.age = age
       }
       public User() {}
   }
   ```

2. Create a producer with a `struct` schema and send messages.

   ```java
   Producer<User> producer = client.newProducer(Schema.AVRO(User.class)).create();
   producer.newMessage().value(new User(“pulsar-user”, 1)).send();
   ```

3. Create a consumer with a `struct` schema and receive messages

   ```java
   Consumer<User> consumer = client.newConsumer(Schema.AVRO(User.class)).subscribe();
   User user = consumer.receive().getValue();
   ```

</TabItem>
<TabItem value="generic">

Sometimes applications do not have pre-defined structs, and you can use this method to define schema and access data.

You can define the `struct` schema using the `GenericSchemaBuilder`, generate a generic struct using `GenericRecordBuilder`, and consume messages into `GenericRecord`.

**Example** 

1. Use `RecordSchemaBuilder` to build a schema.

   ```java
   RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
   recordSchemaBuilder.field("intField").type(SchemaType.INT32);
   SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
   
   Consumer<GenericRecord> consumer = client.newConsumer(Schema.generic(schemaInfo))
        .topic(topicName)
        .subscriptionName(subscriptionName)
        .subscribe();
   Producer<GenericRecord> producer = client.newProducer(Schema.generic(schemaInfo))
        .topic(topicName)
        .create();
   ```

2. Use `RecordBuilder` to build the struct records.

   ```java
   GenericSchemaImpl schema = GenericAvroSchema.of(schemaInfo);
   // send message
   GenericRecord record = schema.newRecordBuilder().set("intField", 32).build();
   producer.newMessage().value(record).send();
   // receive message
   Message<GenericRecord> msg = consumer.receive();
   
   Assert.assertEquals(msg.getValue().getField("intField"), 32);
   ```

</TabItem>
<TabItem value="SchemaDefinition">

You can define the `schemaDefinition` to generate a `struct` schema.

**Example** 

1. Create the _User_ class to define the messages sent to Pulsar topics.

   ```java
   public static class User {
       public String name;
       public int age;
       public User(String name, int age) {
 	this.name = name;
	this.age = age
       }
       public User() {}
   }
   ```

2. Create a producer with a `SchemaDefinition` and send messages.

   ```java
   SchemaDefinition<User> schemaDefinition = SchemaDefinition.<User>builder().withPojo(User.class).build();
   Producer<User> producer = client.newProducer(Schema.AVRO(schemaDefinition)).create();
   producer.newMessage().value(new User (“pulsar-user”, 1)).send();
   ```

3. Create a consumer with a `SchemaDefinition` schema and receive messages.

   ```java
   SchemaDefinition<User> schemaDefinition = SchemaDefinition.<User>builder().withPojo(User.class).build();
   Consumer<User> consumer = client.newConsumer(Schema.AVRO(schemaDefinition)).subscribe();
   User user = consumer.receive().getValue();
   ```

</TabItem>
</Tabs>
````

### Avro

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>

<TabItem value="Java">

Suppose you have a `SensorReading` class as follows, and you'd like to transmit it over a Pulsar topic.

```java
public class SensorReading {
    public float temperature;

    public SensorReading(float temperature) {
        this.temperature = temperature;
    }

    // A no-arg constructor is required
    public SensorReading() {
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }
}
```

Create a `Producer<SensorReading>` (or `Consumer<SensorReading>`) like this:

```java
Producer<SensorReading> producer = client.newProducer(AvroSchema.of(SensorReading.class))
        .topic("sensor-readings")
        .create();
```

</TabItem>
<TabItem value="C++">

  ```cpp
  // Send messages
  static const std::string exampleSchema =
      "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
      "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
  Producer producer;
  ProducerConfiguration producerConf;
  producerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
  client.createProducer("topic-avro", producerConf, producer);

  // Receive messages
  static const std::string exampleSchema =
      "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
      "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
  ConsumerConfiguration consumerConf;
  Consumer consumer;
  consumerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
  client.subscribe("topic-avro", "sub-2", consumerConf, consumer)
  ```

</TabItem>
<TabItem value="Python">

You can declare an `AvroSchema` using Python through one of the following methods.

**Method 1: Record**

Declare an `AvroSchema` by passing a class that inherits from `pulsar.schema.Record` and defines the fields as class variables.

```python
class Example(Record):
    a = Integer()
    b = Integer()

producer = client.create_producer(
'avro-schema-topic',
schema=AvroSchema(Example))
r = Example(a=1, b=2)
producer.send(r)

consumer = client.subscribe(
'avro-schema-topic',
		'sub',
		schema=AvroSchema(Example))
msg = consumer.receive()
e = msg.value()
```

**Method 2: JSON definition**

1. Declare an `AvroSchema` using JSON. In this case, Avro schemas are defined using JSON.

   Below is an example of `AvroSchema` defined using a JSON file (`company.avsc`).

   ```json
   {
       "doc": "this is doc",
       "namespace": "example.avro",
       "type": "record",
       "name": "Company",
       "fields": [
           {"name": "name", "type": ["null", "string"]},
           {"name": "address", "type": ["null", "string"]},
           {"name": "employees", "type": ["null", {"type": "array", "items": {
               "type": "record",
               "name": "Employee",
               "fields": [
                   {"name": "name", "type": ["null", "string"]},
                   {"name": "age", "type": ["null", "int"]}
               ]
           }}]},
           {"name": "labels", "type": ["null", {"type": "map", "values": "string"}]}
       ]
   }
   ```

2. Load a schema definition from a file by using [`avro.schema`](https://avro.apache.org/docs/current/getting-started-python/) or [`fastavro.schema`](https://fastavro.readthedocs.io/en/latest/schema.html#fastavro._schema_py.load_schema).

   If you use the [JSON definition](#method-2-json-definition) method to declare an `AvroSchema`, you need to:
   - Use [Python dict](https://developers.google.com/edu/python/dict-files) to produce and consume messages, which is different from using the [Record](#method-1-record) method.
   - Set the value of the `_record_cls` parameter to `None` when generating an `AvroSchema` object.

   **Example**

   ```python
   from fastavro.schema import load_schema
   from pulsar.schema import *
   schema_definition = load_schema("examples/company.avsc")
   avro_schema = AvroSchema(None, schema_definition=schema_definition)
   producer = client.create_producer(
       topic=topic,
       schema=avro_schema)
   consumer = client.subscribe(topic, 'test', schema=avro_schema)
   company = {
       "name": "company-name" + str(i),
       "address": 'xxx road xxx street ' + str(i),
       "employees": [
           {"name": "user" + str(i), "age": 20 + i},
           {"name": "user" + str(i), "age": 30 + i},
           {"name": "user" + str(i), "age": 35 + i},
       ],
       "labels": {
           "industry": "software" + str(i),
           "scale": ">100",
           "funds": "1000000.0"
       }
   }
   producer.send(company)
   msg = consumer.receive()
   # Users could get a dict object by `value()` method.
   msg.value()
   ```

</TabItem>
<TabItem value="Go">

Suppose you have an `avroExampleStruct` class as follows, and you'd like to transmit it over a Pulsar topic.

```go
  type avroExampleStruct struct {
  ID   int
  Name string
}
```

1. Add an `avroSchemaDef` like this:

   ```go
   var (
     exampleSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
     "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
   )
   ```

2. Create producer and consumer to send/receive messages:

   ```go
   //Create producer and send message
   producer, err := client.CreateProducer(pulsar.ProducerOptions{
     Topic:  "my-topic",
     Schema: pulsar.NewAvroSchema(exampleSchemaDef, nil),
   })

   msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
     Value: avroExampleStruct{
        ID:   10,
        Name: "avroExampleStruct",
     },
   })

   //Create Consumer and receive message
   consumer, err := client.Subscribe(pulsar.ConsumerOptions{
     Topic:            "my-topic",
     Schema:           pulsar.NewAvroSchema(exampleSchemaDef, nil),
     SubscriptionName: "my-sub",
     Type:             pulsar.Shared,
   })
   message, err := consumer.Receive(context.Background())
   ```

</TabItem>
</Tabs>
````

### JSON

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>

<TabItem value="Java">

Similar to use `AvroSchema`, you can declare a `JsonSchema` by passing a class. The only difference is to use  `JsonSchema` instead of `AvroSchema` when defining the schema type, as shown below. For how to use `AvroSchema` via record, see [Method 1 - Record](#method-1-record).

```java
static class SchemaDema {
   public String name;
   public int age;
}

Producer<SchemaDema> producer = pulsarClient.newProducer(Schema.JSON(SchemaDema.class))
       .topic("my-topic")
       .create();
Consumer<SchemaDema> consumer = pulsarClient.newConsumer(Schema.JSON(SchemaDema.class))
       .topic("my-topic")
       .subscriptionName("my-sub")
       .subscribe();

SchemaDema schemaDema = new SchemaDema();
schemaDema.name = "puslar";
schemaDema.age = 20;
producer.newMessage().value(schemaDema).send();

Message<SchemaDema> message = consumer.receive(5, TimeUnit.SECONDS);
```

</TabItem>
<TabItem value="C++">

To declare a `JSON` schema using C++, do the following:

1. Pass a JSON string like this:

   ```cpp
   Std::string jsonSchema = R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
   SchemaInfo schemaInfo = SchemaInfo(JSON, "JSON", jsonSchema);
   ```

2. Create a producer and use it to send messages.

   ```cpp
   client.createProducer("my-topic", ProducerConfiguration().setSchema(schemaInfo), producer);
   std::string jsonData = "{\"re\":2.1,\"im\":1.23}";
   producer.send(MessageBuilder().setContent(std::move(jsonData)).build());
   ```

3. Create consumer and receive message.

   ```cpp
   Consumer consumer;
   client.subscribe("my-topic", "my-sub", ConsumerConfiguration().setSchema(schemaInfo), consumer);
   Message msg;
   consumer.receive(msg);
   ```

</TabItem>
<TabItem value="Python">

You can declare a `JsonSchema` by passing a class that inherits from `pulsar.schema.Record` and defines the fields as class variables. This is similar to using `AvroSchema`. The only difference is to use  `JsonSchema` instead of `AvroSchema` when defining schema type, as shown below. For how to use `AvroSchema` via record, see [#method-1-record).

```python
producer = client.create_producer(
'avro-schema-topic',
schema=JsonSchema(Example))

consumer = client.subscribe(
	'avro-schema-topic',
	'sub',
	schema=JsonSchema(Example))
```

</TabItem>
<TabItem value="Go">

Suppose you have a `avroExampleStruct` class as follows, and you'd like to transmit it as JSON form over a Pulsar topic.

```go
type jsonExampleStruct struct {
  ID   int    `json:"id"`
  Name string `json:"name"`
}
```

1. Add a `jsonSchemaDef` like this:

   ```go
   jsonSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
  "\"fields\":[{\"name\":\"ID\",\"type\":\"int\"},{\"name\":\"Name\",\"type\":\"string\"}]}"
   ```

2. Create a producer/consumer to send/receive messages:

   ```go
   //Create producer and send message
   producer, err := client.CreateProducer(pulsar.ProducerOptions{
     Topic:  "my-topic",
     Schema: pulsar.NewJSONSchema(jsonSchemaDef, nil),
   })

   msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
     Value: jsonExampleStruct{
        ID:   10,
        Name: "jsonExampleStruct",
     },
   })

   //Create Consumer and receive message
   consumer, err := client.Subscribe(pulsar.ConsumerOptions{
     Topic:            "my-topic",
     Schema:           pulsar.NewJSONSchema(jsonSchemaDef, nil),
     SubscriptionName: "my-sub",
     Type:             pulsar.Exclusive,
   })
   message, err := consumer.Receive(context.Background())
   ```

</TabItem>
</Tabs>
````

### ProtobufNative

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"}]}>

<TabItem value="Java">

The following example shows how to create a producer/consumer with a ProtobufNative schema using Java.

1. Generate the `DemoMessage` class using Protobuf3 or later versions.

   ```protobuf
   syntax = "proto3";
   message DemoMessage {
      string stringField = 1;
      double doubleField = 2;
      int32 intField = 6;
      TestEnum testEnum = 4;
      SubMessage nestedField = 5;
      repeated string repeatedField = 10;
      proto.external.ExternalMessage externalMessage = 11;
   }

2. Create a producer/consumer to send/receive messages.

   ```java
   Producer<DemoMessage> producer = pulsarClient.newProducer(Schema.PROTOBUF_NATIVE(DemoMessage.class))
       .topic("my-topic")
       .create();
   Consumer<DemoMessage> consumer = pulsarClient.newConsumer(Schema.PROTOBUF_NATIVE(DemoMessage.class))
       .topic("my-topic")
       .subscriptionName("my-sub")
       .subscribe();

   SchemaDema schemaDema = new SchemaDema();
   schemaDema.name = "puslar";
   schemaDema.age = 20;
   producer.newMessage().value(DemoMessage.newBuilder().setStringField("string-field-value")
       .setIntField(1).build()).send();

   Message<DemoMessage> message = consumer.receive(5, TimeUnit.SECONDS);
   ```
  
</TabItem>
<TabItem value="C++">

The following example shows how to create a producer/consumer with a ProtobufNative schema.

1. Generate the `User` class using Protobuf3 or later versions.

   ```protobuf
   syntax = "proto3";

   message User {
       string name = 1;
       int32 age = 2;
   }
   ```

2. Include the `ProtobufNativeSchema.h` in your source code. Ensure the Protobuf dependency has been added to your project.

   ```cpp
   #include <pulsar/ProtobufNativeSchema.h>
   ```

3. Create a producer to send a `User` instance.

   ```cpp
   ProducerConfiguration producerConf;
   producerConf.setSchema(createProtobufNativeSchema(User::GetDescriptor()));
   Producer producer;
   client.createProducer("topic-protobuf", producerConf, producer);
   User user;
   user.set_name("my-name");
   user.set_age(10);
   std::string content;
   user.SerializeToString(&content);
   producer.send(MessageBuilder().setContent(content).build());
   ```

4. Create a consumer to receive a `User` instance.

   ```cpp
   ConsumerConfiguration consumerConf;
   consumerConf.setSchema(createProtobufNativeSchema(User::GetDescriptor()));
   consumerConf.setSubscriptionInitialPosition(InitialPositionEarliest);
   Consumer consumer;
   client.subscribe("topic-protobuf", "my-sub", consumerConf, consumer);
   Message msg;
   consumer.receive(msg);
   User user2;
   user2.ParseFromArray(msg.getData(), msg.getLength());
   ```

</TabItem>
</Tabs>
````

### Protobuf

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"C++","value":"C++"},{"label":"Go","value":"Go"}]}>

<TabItem value="Java">

Constructing a protobuf schema using Java is similar to construct a `ProtobufNative` schema. The only difference is to use `PROTOBUF` instead of `PROTOBUF_NATIVE` when defining schema type as shown below.

1. Generate the `DemoMessage` class using Protobuf3 or later versions.

   ```protobuf
   syntax = "proto3";
   message DemoMessage {
      string stringField = 1;
      double doubleField = 2;
      int32 intField = 6;
      TestEnum testEnum = 4;
      SubMessage nestedField = 5;
      repeated string repeatedField = 10;
      proto.external.ExternalMessage externalMessage = 11;
   }

2. Create a producer/consumer to send/receive messages.

   ```java
   Producer<DemoMessage> producer = pulsarClient.newProducer(Schema.PROTOBUF(DemoMessage.class))
          .topic("my-topic")
          .create();
   Consumer<DemoMessage> consumer = pulsarClient.newConsumer(Schema.PROTOBUF(DemoMessage.class))
          .topic("my-topic")
          .subscriptionName("my-sub")
          .subscribe();

   SchemaDema schemaDema = new SchemaDema();
   schemaDema.name = "puslar";
   schemaDema.age = 20;
   producer.newMessage().value(DemoMessage.newBuilder().setStringField("string-field-value")
       .setIntField(1).build()).send();

   Message<DemoMessage> message = consumer.receive(5, TimeUnit.SECONDS);
```

</TabItem>
<TabItem value="C++">

Constructing a protobuf schema using C++ is similar to that using `JSON`. The only difference is to use `PROTOBUF` instead of `JSON` when defining the schema type as shown below.

```cpp
std::string jsonSchema =
   R"({"type":"record","name":"cpx","fields":[{"name":"re","type":"double"},{"name":"im","type":"double"}]})";
SchemaInfo schemaInfo = SchemaInfo(pulsar::PROTOBUF, "PROTOBUF", jsonSchema);
```

1. Create a producer to send messages.

   ```cpp
   Producer producer;
   client.createProducer("my-topic", ProducerConfiguration().setSchema(schemaInfo), producer);
   std::string jsonData = "{\"re\":2.1,\"im\":1.23}";
   producer.send(MessageBuilder().setContent(std::move(jsonData)).build());
   ```

2. Create a consumer to receive messages.

   ```cpp
   Consumer consumer;
   client.subscribe("my-topic", "my-sub", ConsumerConfiguration().setSchema(schemaInfo),   consumer);
   Message msg;
   consumer.receive(msg);
   ```

</TabItem>
<TabItem value="Go">

Suppose you have a `protobufDemo` class as follows, and you'd like to transmit it in JSON form over a Pulsar topic.

```go
type protobufDemo struct {
  Num                  int32    `protobuf:"varint,1,opt,name=num,proto3" json:"num,omitempty"`
  Msf                  string   `protobuf:"bytes,2,opt,name=msf,proto3" json:"msf,omitempty"`
  XXX_NoUnkeyedLiteral struct{} `json:"-"`
  XXX_unrecognized     []byte   `json:"-"`
  XXX_sizecache        int32    `json:"-"`
}
```

1. Add a `protoSchemaDef` like this:

   ```go
   var (
     protoSchemaDef = "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\"," +
        "\"fields\":[{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"msf\",\"type\":\"string\"}]}"
   )
   ```

2. Create a producer/consumer to send/receive messages:

   ```go
   psProducer := pulsar.NewProtoSchema(protoSchemaDef, nil)
   producer, err := client.CreateProducer(pulsar.ProducerOptions{
     Topic:  "proto",
     Schema: psProducer,
   })
   msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
     Value: &protobufDemo{
        Num: 100,
        Msf: "pulsar",
     },
   })
   psConsumer := pulsar.NewProtoSchema(protoSchemaDef, nil)
   consumer, err := client.Subscribe(pulsar.ConsumerOptions{
     Topic:                       "proto",
     SubscriptionName:            "sub-1",
     Schema:                      psConsumer,
     SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
   })
   msg, err := consumer.Receive(context.Background())
   ```

</TabItem>
</Tabs>
````

### Native Avro

This example shows how to construct a [native Avro schema](schema-understand.md#struct-schema).

```java
org.apache.avro.Schema nativeAvroSchema = … ;
Producer<byte[]> producer = pulsarClient.newProducer().topic("ingress").create();
byte[] content = … ;
producer.newMessage(Schema.NATIVE_AVRO(nativeAvroSchema)).value(content).send();
```

### AUTO_PRODUCE

Suppose you have a Pulsar topic _P_, a producer processing messages from a Kafka topic _K_, an application reading the messages from _K_ and writing the messages to _P_.

This example shows how to construct an [AUTO_PRODUCE](schema-understand.md#auto-schema) schema to verify whether the bytes produced by _K_ can be sent to _P_.

```java
Produce<byte[]> pulsarProducer = client.newProducer(Schema.AUTO_PRODUCE())
    …
    .create();
byte[] kafkaMessageBytes = … ; 
pulsarProducer.produce(kafkaMessageBytes);
```

### AUTO_CONSUME

Suppose you have a Pulsar topic _P_ and a consumer _MySQL_ that receives messages from _P_, and you want to check if these messages have the information that your application needs to count.

This example shows how to construct an [AUTO_CONSUME schema](schema-understand.md#auto-schema) to verify whether the bytes produced by _P_ can be sent to _MySQL_.

```java
Consumer<GenericRecord> pulsarConsumer = client.newConsumer(Schema.AUTO_CONSUME())
    …
    .subscribe();

Message<GenericRecord> msg = consumer.receive() ; 
GenericRecord record = msg.getValue();
record.getFields().forEach((field -> {
   if (field.getName().equals("theNeedFieldName")) {
       Object recordField = record.getField(field);
       //Do some things 
   }
}));
```

## Customize schema storage

By default, Pulsar stores various data types of schemas in [Apache BookKeeper](https://bookkeeper.apache.org) deployed alongside Pulsar. Alternatively, you can use another storage system if needed. 

To use a non-default (non-BookKeeper) storage system for Pulsar schemas, you need to implement the following Java interfaces before [deploying custom schema storage](#deploy-custom-schema-storage): 
* [SchemaStorage interface](#implement-schemastorage-interface) 
* [SchemaStorageFactory interface](#implement-schemastoragefactory-interface)

### Implement `SchemaStorage` interface

The `SchemaStorage` interface has the following methods:

```java
public interface SchemaStorage {
    // How schemas are updated
    CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash);

    // How schemas are fetched from storage
    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);

    // How schemas are deleted
    CompletableFuture<SchemaVersion> delete(String key);

    // Utility method for converting a schema version byte array to a SchemaVersion object
    SchemaVersion versionFromBytes(byte[] version);

    // Startup behavior for the schema storage client
    void start() throws Exception;

    // Shutdown behavior for the schema storage client
    void close() throws Exception;
}
```

:::tip

For a complete example of **schema storage** implementation, see [BookKeeperSchemaStorage](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorage.java) class.

:::

### Implement `SchemaStorageFactory` interface 

The `SchemaStorageFactory` interface has the following method:

```java
public interface SchemaStorageFactory {
    @NotNull
    SchemaStorage create(PulsarService pulsar) throws Exception;
}
```

:::tip

For a complete example of **schema storage factory** implementation, see [BookKeeperSchemaStorageFactory](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorageFactory.java) class.

:::

### Deploy custom schema storage

To use your custom schema storage implementation, perform the following steps.

1. Package the implementation in a [JAR](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html) file.
   
2. Add the JAR file to the `lib` folder in your Pulsar binary or source distribution.
   
3. Change the `schemaRegistryStorageClassName` configuration in the `conf/broker.conf` file to your custom factory class.
      
4. Start Pulsar.