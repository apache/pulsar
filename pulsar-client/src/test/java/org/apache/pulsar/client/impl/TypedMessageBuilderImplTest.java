package org.apache.pulsar.client.impl;

import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.common.schema.KeyValue;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class TypedMessageBuilderImplTest {

    @Mock
    protected ProducerBase producerBase;

    @Test
    public void testValue() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);
        Map<String, String> properties = Maps.newHashMap();
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = new KeyValue<>(foo, bar);

        // Check kv.encoding.type default, not set value
        TypedMessageBuilderImpl<KeyValue>  typedMessageBuilder = (TypedMessageBuilderImpl)typedMessageBuilderImpl.value(keyValue);
        ByteBuffer content = typedMessageBuilder.getContent();
        byte[] contentByte = new byte[content.remaining()];
        content.get(contentByte);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>  decodeKeyValue = keyValueSchema.decode(contentByte);
        assertEquals(decodeKeyValue.getKey(), foo);
        assertEquals(decodeKeyValue.getValue(), bar);

        // Check kv.encoding.type INLINE
        properties.put("kv.encoding.type", "INLINE");
        keyValueSchema.getSchemaInfo().setProperties(properties);
        typedMessageBuilder = (TypedMessageBuilderImpl)typedMessageBuilderImpl.value(keyValue);
        content = typedMessageBuilder.getContent();
        contentByte = new byte[content.remaining()];
        content.get(contentByte);
        decodeKeyValue = keyValueSchema.decode(contentByte);
        assertEquals(decodeKeyValue.getKey(), foo);
        assertEquals(decodeKeyValue.getValue(), bar);

        // Check kv.encoding.type SEPARATED
        properties.put("kv.encoding.type", "SEPARATED");
        keyValueSchema.getSchemaInfo().setProperties(properties);
        typedMessageBuilder = (TypedMessageBuilderImpl)typedMessageBuilderImpl.value(keyValue);
        content = typedMessageBuilder.getContent();
        contentByte = new byte[content.remaining()];
        content.get(contentByte);
        decodeKeyValue = keyValueSchema.decode(fooSchema.encode(foo), contentByte);
        assertEquals(decodeKeyValue.getValue(), bar);

    }

}
