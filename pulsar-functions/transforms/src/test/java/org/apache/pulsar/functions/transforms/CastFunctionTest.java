package org.apache.pulsar.functions.transforms;

import static org.apache.pulsar.functions.transforms.Utils.createTestAvroKeyValueRecord;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class CastFunctionTest {

    @Test
    void testKeyValueAvroToString() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Map<String, Object> config = ImmutableMap.of(
                "key-schema-type", "STRING",
                "value-schema-type", "STRING");
        Utils.TestContext context = new Utils.TestContext(record, config);

        CastFunction castFunction = new CastFunction();
        castFunction.initialize(context);
        castFunction.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();
        KeyValueSchema messageSchema = (KeyValueSchema) message.getSchema();
        KeyValue messageValue = (KeyValue) message.getValue();

        assertSame(messageSchema.getKeySchema(), Schema.STRING);
        assertEquals(messageValue.getKey(), "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\"}");
        assertSame(messageSchema.getValueSchema(), Schema.STRING);
        assertEquals(messageValue.getValue(), "{\"valueField1\": \"value1\", \"valueField2\": \"value2\", "
                + "\"valueField3\": \"value3\"}");
    }


}
