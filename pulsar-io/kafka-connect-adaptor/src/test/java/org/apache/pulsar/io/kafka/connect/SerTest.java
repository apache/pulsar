package org.apache.pulsar.io.kafka.connect;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Slf4j
public class SerTest {

    @Test
    public void test() {

        Schema schema = KeyValueSchema.of(new KafkaSchemaWrappedSchema(null, null),
                new KafkaSchemaWrappedSchema(null, null), KeyValueEncodingType.SEPARATED);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream);
            oos.writeObject(schema);
            oos.flush();
            oos.close();

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(byteArrayInputStream);
            Schema schema2 = (Schema) ois.readObject();
            log.info("deserializable schema: {}, classLoader: {}",
                    schema.getClass().getName(), schema.getClass().getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
