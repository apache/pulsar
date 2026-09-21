# PIP-69: Schema design for Go client

- Status: Proposal
- Author: XiaoLong Ran
- Pull Request:
- Mailing List discussion:
- Release:

## Motivation

Type safety is extremely important in any application built around a message bus like Pulsar. Currently, Apache Pulsar supports the function of Schema Registry. And Java, CPP and Python clients already support schema registry related functions, In order to further improve the function of Go Client, we need to support the function of Schema Registry in Go Client.

## Implementation

### Schema

In the implemention of Schema, there are three important struct: SchemaType, SchemaInfo and Schema.

#### SchemaType

The `SchemaType` is used to describe which types the current Schema Registry supports. Pulsar supports various schema types, which are mainly divided into two categories:

- Primitive type
- Complex type

The specific definition is as follows:

```
type SchemaType int

const (
	NONE         SchemaType = iota //No schema defined
	STRING                         //Simple String encoding with UTF-8
	JSON                           //JSON object encoding and validation
	PROTOBUF                       //Protobuf message encoding and decoding
	AVRO                           //Serialize and deserialize via Avro
	BOOLEAN                        //
	INT8                           //A 8-byte integer.
	INT16                          //A 16-byte integer.
	INT32                          //A 32-byte integer.
	INT64                          //A 64-byte integer.
	FLOAT                          //A float number.
	DOUBLE                         //A double number
	KEY_VALUE                      //A Schema that contains Key Schema and Value Schema.
	BYTES        = -1              //A bytes array.
	AUTO         = -2              //
	AUTO_CONSUME = -3              //Auto Consume Type.
	AUTO_PUBLISH = -4              //Auto Publish Type.
)
```

#### SchemaInfo

Pulsar schema is defined in a data structure called SchemaInfo. The SchemaInfo is stored and enforced on a per-topic basis and cannot be stored at the namespace or tenant level.


```
type SchemaInfo struct {
	Name   string
	Schema string
	Type   SchemaType
	Properties map[string]string
}
```

#### Schema

The `Schema` interface is as follows, all types of Schema will implement this interface,

```
type Schema interface {
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
	Validate(message []byte) error
	GetSchemaInfo() *SchemaInfo
}
```

Below we introduce Json Schema as a demo:

```
type AvroCodec struct {
	Codec *goavro.Codec
}

type JsonSchema struct {
	AvroCodec
	SchemaInfo
}

func NewJsonSchema(jsonAvroSchemaDef string, properties map[string]string) *JsonSchema {
	js := new(JsonSchema)
	avroCodec, err := initAvroCodec(jsonAvroSchemaDef)
	if err != nil {
		log.Fatalf("init codec error:%v", err)
	}
	schemaDef := NewSchemaDefinition(avroCodec)
	js.SchemaInfo.Schema = schemaDef.Codec.Schema()
	js.SchemaInfo.Type = JSON
	js.SchemaInfo.Properties = properties
	js.SchemaInfo.Name = "Json"
	return js
}
```

And the `JsonSchema` will implement the `Schema` interface.

### Producer

Here, we can have two implementations, one is to use `Schema` as one of the options of ProducerOptions, as shown below:

```
type ProducerOptions struct {
  Schema
}
```

The other is to add a new Create Producer function to the Client interface and pass in the Schema as a parameter, as shown below:

```
type Client interface {
	CreateProducerWithSchema(ProducerOptions, Schema) (Producer, error)
}
```

Here I prefer to use the first method. The implementation of Consumer and Reader is similar to this.

### Consumer

```
type ConsumerOptions struct {
  Schema
}
```

### Reader

```
type ReaderOptions struct {
  Schema
}
```
