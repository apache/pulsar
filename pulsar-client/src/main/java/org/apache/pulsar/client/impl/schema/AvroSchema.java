/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.reader.AvroReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An AVRO schema implementation.
 */
@Slf4j
public class AvroSchema<T> extends StructSchema<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchema.class);

    // the aim to fix avro's bug
    // https://issues.apache.org/jira/browse/AVRO-1891 bug address explain
    // fix the avro logical type read and write
    static {
        ReflectData reflectDataAllowNull = ReflectData.AllowNull.get();

        reflectDataAllowNull.addLogicalTypeConversion(new Conversions.DecimalConversion());
        reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.DateConversion());
        reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());

        ReflectData reflectDataNotAllowNull = ReflectData.get();

        reflectDataNotAllowNull.addLogicalTypeConversion(new Conversions.DecimalConversion());
        reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.DateConversion());
        reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    }

    private ClassLoader pojoClassLoader;

    private AvroSchema(SchemaInfo schemaInfo, ClassLoader pojoClassLoader) {
        super(schemaInfo);
        this.pojoClassLoader = pojoClassLoader;
        setReader(new AvroReader<>(schema, pojoClassLoader));
        setWriter(new AvroWriter<>(schema));
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public Schema<T> clone() {
        Schema<T> schema = new AvroSchema<>(schemaInfo, pojoClassLoader);
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    public static <T> AvroSchema<T> of(SchemaDefinition<T> schemaDefinition) {
        ClassLoader pojoClassLoader = null;
        if (schemaDefinition.getPojo() != null) {
            pojoClassLoader = schemaDefinition.getPojo().getClassLoader();
        }
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO), pojoClassLoader);
    }

    public static <T> AvroSchema<T> of(Class<T> pojo) {
        return AvroSchema.of(SchemaDefinition.<T>builder().withPojo(pojo).build());
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        ClassLoader pojoClassLoader = null;
        if (pojo != null) {
            pojoClassLoader = pojo.getClassLoader();
        }
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo).withProperties(properties).build();
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO), pojoClassLoader);
    }

    @Override
    protected SchemaReader<T> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}, schemaInfo: {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                schemaInfo.getSchemaDefinition(), schemaInfo.toString());
            return new AvroReader<>(parseAvroSchema(schemaInfo.getSchemaDefinition()), schema, pojoClassLoader);
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                this.schemaInfo.getSchemaDefinition());
            return reader;
        }
    }

}
