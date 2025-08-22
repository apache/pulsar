# PIP-71: Pulsar SQL migrate SchemaHandle to presto decoder

- Status: Proposal
- Author: [@hnail](https://github.com/hnail)
- Pull Request: https://github.com/apache/pulsar/pull/8422
- Mailing List discussion:
- Release:

## Motivation

In the current version, pulsar-presto deserialize fields using SchemaHandler, but this causes the following restrictions :

- **Metadata**: current nested field is dissociated with presto ParameterizedType, It treats the nested field as a separate field, so presto compiler can't understand the type hierarchy. the nested field should be Row type in presto (e.g.  Hive struct type support). In the same way，array \ map type also should associate with presto ParameterizedTypes.
- **Decoder**: SchemaHandler is hard to work with  `RecordCursor.getObject()` to  support ROW,MAP,ARRAY .etc

The **motivations** of this pull request :

- PulsarMetadata takes advantage of ParameterizedType to describe row/array/map Type instead of resolving nested columns in the pulsar-presto connecter.
- Customize RowDecoder | RowDecoderFactory | ColumnDecoder to work with pulsar interface, and with some of our own extensions compare with presto original version, we can support more type for backward compatibility (e.g. TIMESTAMP\DATE\TIME\Real\ARRAY\MAP\ROW support).
- Decouple Avro or schema type with a pulsar-presto main module (RecordSet, ConnectorMetadata .etc ), aim to friendly with other schema types ( ProtobufNative (https://github.com/apache/pulsar/pull/8372) 、thrift, etc..).

## Implementation

`PulsarDispatchingRowDecoderFactory` create `PulsarRowDecoderFactory` by `SchemaInfo.SchemaType` , `PulsarRowDecoderFactory` extract  `ColumnMetadata ` and create `RowDecoder` by SchemaInfo , `PulsarRowDecoder ` decode pulsar ByteBuf to Map<DecoderColumnHandle, FieldValueProvider> depend on  `ColumnDecoder`,`FieldValueProvider` Implementor prepare method getXXX()  for presto runtime code-generation。

### PulsarDispatchingRowDecoderFactory

PulsarDispatchingRowDecoderFactory is a factory of PulsarRowDecoderFactory based  SchemaInfo.SchemaType, all pulsar-presto core modules interact with the decoder by this interface.

```
public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns) 
  public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType) 
```

## PulsarRowDecoderFactory

PulsarRowDecoderFactory is a factory to extract ColumnMetadata and create RowDecoder by SchemaInfo.

```	
	// extract ColumnMetadata from pulsar SchemaInfo and HandleKeyValueType
public List<ColumnMetadata> extractColumnMetadata(TopicName topicName, SchemaInfo schemaInfo, PulsarColumnHandle.HandleKeyValueType handleKeyValueType);
// createRowDecoder RowDecoder by pulsar SchemaInfo and column DecoderColumnHandles
 public PulsarRowDecoder createRowDecoder(TopicName topicName, SchemaInfo schemaInfo, Set<DecoderColumnHandle> columns);
```

### PulsarRowDecoder

PulsarRowDecoder is the interface decode pulsar ByteBuf to Map<DecoderColumnHandle, FieldValueProvider> depend on ColumnDecoder.

```
  //decode byteBuf to Map<DecoderColumnHandle, FieldValueProvider>
   public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf);
```

### ColumnDecoder

ColumnDecoder is the factory create FieldValueProviders by row meta , FieldValueProviders can prepare method getBoolean()/getLong()/getDouble()/getSlice()/getBlock() for presto runtime code-generation. we do some extensions to support more type for backward compatible compare with presto original version :
- PulsarAvroColumnDecoder : add support TIMESTAMP,DATE,TIME,Real
- PulsarJsonFieldDecoder : add support array,map,row,TIMESTAMP,DATE,TIME,Real

### DecoderTest 

Add separate decoder unit-tests to work with our puslar customized interface.

## PrototypeCode
https://github.com/apache/pulsar/pull/8422

## Future improve

- Check schema cyclic definitions which may case java.lang.StackOverflowError in PulsarRowDecoderFactory.extractColumnMetadata().
- Pulsar-SQL support `ProtobufNativeSchema`
