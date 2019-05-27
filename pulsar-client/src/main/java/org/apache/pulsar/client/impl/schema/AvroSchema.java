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
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.reader.AvroReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
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

    //      the aim to fix avro's bug
//      https://issues.apache.org/jira/browse/AVRO-1891  bug address explain
//      fix the avro logical type read and write
    static {
        try {
            ReflectData reflectDataAllowNull = ReflectData.AllowNull.get();

            reflectDataAllowNull.addLogicalTypeConversion(new Conversions.DecimalConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.DateConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.LossyTimeMicrosConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.LossyTimestampMicrosConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampConversion());
            reflectDataAllowNull.addLogicalTypeConversion(new TimeConversions.TimeConversion());

            ReflectData reflectDataNotAllowNull = ReflectData.get();

            reflectDataNotAllowNull.addLogicalTypeConversion(new Conversions.DecimalConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.DateConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.LossyTimeMicrosConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.LossyTimestampMicrosConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
            reflectDataNotAllowNull.addLogicalTypeConversion(new TimeConversions.TimeConversion());
        } catch (Throwable t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Avro logical types are not available. If you are going to use avro logical types, " +
                        "you can include `joda-time` in your dependency.");
            }
        }
    }

    private AvroSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        setReader(new AvroReader<>(schema));
        setWriter(new AvroWriter<>(schema));
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    public static <T> AvroSchema<T> of(SchemaDefinition<T> schemaDefinition) {
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO));
    }

    public static <T> AvroSchema<T> of(Class<T> pojo) {
        return AvroSchema.of(SchemaDefinition.<T>builder().withPojo(pojo).build());
    }

    public static <T> AvroSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        SchemaDefinition<T> schemaDefinition = SchemaDefinition.<T>builder().withPojo(pojo).withProperties(properties).build();
        return new AvroSchema<>(parseSchemaInfo(schemaDefinition, SchemaType.AVRO));
    }

    @Override
    protected SchemaReader<T> loadReader(byte[] schemaVersion) {
        SchemaInfo schemaInfo = schemaInfoProvider.getSchemaByVersion(schemaVersion);
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion),
                schemaInfo.getSchemaDefinition());
            return new AvroReader<>(parseAvroSchema(schemaInfo.getSchemaDefinition()), schema);
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                SchemaUtils.getStringSchemaVersion(schemaVersion),
                this.schemaInfo.getSchemaDefinition());
            return reader;
        }
    }

}
