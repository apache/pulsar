package org.apache.pulsar.functions.transforms;

import static org.apache.pulsar.functions.transforms.Utils.createTestAvroKeyValueRecord;
import static org.apache.pulsar.functions.transforms.Utils.getRecord;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

public class MergeKeyValueFunctionTest {

    @Test
    void testMergeKeyValueAvro() throws Exception {
        Record<KeyValue<GenericRecord, GenericRecord>> record = createTestAvroKeyValueRecord();
        Utils.TestContext context = new Utils.TestContext(record, null);

        MergeKeyValueFunction function = new MergeKeyValueFunction();
        function.initialize(context);
        function.process(
                AutoConsumeSchema.wrapPrimitiveObject(record.getValue(), SchemaType.KEY_VALUE, new byte[]{}),
                context);

        Utils.TestTypedMessageBuilder<?> message = context.getOutputMessage();

        GenericData.Record read = getRecord(message.getSchema(), (byte[]) message.getValue());
        assertEquals(read.toString(), "{\"keyField1\": \"key1\", \"keyField2\": \"key2\", \"keyField3\": \"key3\", "
                + "\"valueField1\": \"value1\", \"valueField2\": \"value2\", \"valueField3\": \"value3\"}");
    }
}
