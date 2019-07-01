package org.apache.pulsar.sql.presto;

import io.netty.buffer.ByteBufAllocator;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import static org.mockito.Mockito.mock;

@Slf4j
public class TestAvroSchemaHandler {

    @Data
    public static class Foo1 {

        String field1;
    }
    @Data
    public static class Foo2 {

        String field1;

        String field2;
    }


    @Test
    public void testAvroSchemaHandler() throws IOException, PulsarAdminException, NoSuchFieldException {
        Schema schema = ReflectData.AllowNull.get().getSchema(Foo2.class);
        List columnHandles = mock(ArrayList.class);
        PulsarConnectorConfig pulsarConnectorConfig = mock(PulsarConnectorConfig.class);
        TopicName topicName = mock(TopicName.class);
        RawMessage message = mock(RawMessage.class);
        Schema schema1 = ReflectData.AllowNull.get().getSchema(Foo1.class);
        AvroSchemaHandler avroSchemaHandler = new AvroSchemaHandler(schema, columnHandles, pulsarConnectorConfig, topicName);
        byte[] schemaVersion = new byte[8];
        for (int i = 0 ; i<8; i++) {
            schemaVersion[i] = 0;
        }
        ReflectDatumWriter<Foo1> writer;
        BinaryEncoder encoder = null;
        ByteArrayOutputStream byteArrayOutputStream;
        byteArrayOutputStream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, encoder);
        writer = new ReflectDatumWriter<>(schema1);
        Foo1 foo1 = new Foo1();
        foo1.setField1("field1");
        writer.write(foo1, encoder);
        encoder.flush();
        when(message.getSchemaVersion()).thenReturn(schemaVersion);
        byte[] bytes =byteArrayOutputStream.toByteArray();

        when(message.getData()).thenReturn(ByteBufAllocator.DEFAULT
                .buffer(bytes.length, bytes.length).writeBytes(byteArrayOutputStream.toByteArray()));
        Schemas schemas = mock(Schemas.class);
        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        when(pulsarConnectorConfig.getPulsarAdmin()).thenReturn(pulsarAdmin);
        when(pulsarAdmin.schemas()).thenReturn(schemas);
        when(schemas.getSchemaInfo(anyString(), anyLong())).thenReturn(schemaInfo);
        when(schemaInfo.getSchemaDefinition()).thenReturn(schema1.toString());

        Object object  = ((GenericData.Record)avroSchemaHandler.deserialize(message)).get("field1");
        Assert.assertEquals(foo1.field1, ((Utf8)object).toString());

    }
}
