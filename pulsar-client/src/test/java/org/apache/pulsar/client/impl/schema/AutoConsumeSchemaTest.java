package org.apache.pulsar.client.impl.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

@Slf4j
public class AutoConsumeSchemaTest {

    @Test
    public void decodeDataWithNullSchemaVersion() {
        Schema<GenericRecord> autoConsumeSchema = new AutoConsumeSchema();
        byte[] bytes = "bytes data".getBytes();
        MessageImpl<GenericRecord> message = MessageImpl.create(
                new MessageMetadata(), ByteBuffer.wrap(bytes), autoConsumeSchema);
        Assert.assertNull(message.getSchemaVersion());
        GenericRecord genericRecord = message.getValue();
        Assert.assertEquals(genericRecord.getNativeObject(), bytes);
    }

}
