package org.apache.pulsar.client.schemas;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.testng.annotations.Test;

@Slf4j
public class AvroSchemaTest {

    @Data
    @ToString
    private static class Foo {
        private String field1;
        private String field2;
        private int field3;
        private Bar field4;
    }

    @Data
    @ToString
    private static class Bar {
        private boolean field1;
    }

    @Test
    public void testEncodeAndDecode() {
        AvroSchema<Foo> avroSchema = AvroSchema.of(Foo.class, null);

        Foo foo1 = new Foo();
        foo1.setField1("foo");
        foo1.setField2("bar");

        Foo foo2 = new Foo();
        foo2.setField1("foo");
        foo2.setField2("bar");

        byte[] bytes1 = avroSchema.encode(foo1);
        byte[] bytes2 = avroSchema.encode(foo2);


        log.info("o: {}", avroSchema.decode(bytes1));
        log.info("o: {}", avroSchema.decode(bytes2));



    }
}
