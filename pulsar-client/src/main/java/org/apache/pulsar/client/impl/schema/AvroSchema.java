package org.apache.pulsar.client.impl.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
public class AvroSchema<T> implements Schema<T> {

    private SchemaInfo schemaInfo;
    private org.apache.avro.Schema schema;
    private DatumWriter<T> outputDatumWriter;
    private BinaryEncoder encoder;

    ByteArrayOutputStream baos;

    public AvroSchema(Class<T> pojo, Map<String, String> properties) {
        this.schema = ReflectData.get().getSchema(pojo);
        log.info("schema: {}", this.schema.toString());

        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        log.info("schema: {}", parser.parse(this.schema.toString()));

        this.schemaInfo = new SchemaInfo();
        this.schemaInfo.setName("");
        this.schemaInfo.setProperties(properties);
        this.schemaInfo.setType(SchemaType.JSON);
        this.schemaInfo.setSchema(this.schema.toString().getBytes());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        this.outputDatumWriter = new SpecificDatumWriter<T>(this.schema);
        this.encoder = EncoderFactory.get().binaryEncoder(baos, null);

    }

    @Override
    public byte[] encode(T message) {


        try {
            outputDatumWriter.write(message, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public T decode(byte[] bytes) {
        DatumReader<T> datumReader = new SpecificDatumReader<>(this.schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return null;
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {

        return new AvroSchema<>(pojo, properties);
    }
}
