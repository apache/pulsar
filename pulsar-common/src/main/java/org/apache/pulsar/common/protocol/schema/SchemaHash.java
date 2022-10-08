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
package org.apache.pulsar.common.protocol.schema;

import static org.apache.pulsar.common.schema.SchemaType.AUTO;
import static org.apache.pulsar.common.schema.SchemaType.AUTO_CONSUME;
import static org.apache.pulsar.common.schema.SchemaType.AUTO_PUBLISH;
import static org.apache.pulsar.common.schema.SchemaType.AVRO;
import static org.apache.pulsar.common.schema.SchemaType.BOOLEAN;
import static org.apache.pulsar.common.schema.SchemaType.BYTES;
import static org.apache.pulsar.common.schema.SchemaType.DATE;
import static org.apache.pulsar.common.schema.SchemaType.DOUBLE;
import static org.apache.pulsar.common.schema.SchemaType.FLOAT;
import static org.apache.pulsar.common.schema.SchemaType.INSTANT;
import static org.apache.pulsar.common.schema.SchemaType.INT16;
import static org.apache.pulsar.common.schema.SchemaType.INT32;
import static org.apache.pulsar.common.schema.SchemaType.INT64;
import static org.apache.pulsar.common.schema.SchemaType.INT8;
import static org.apache.pulsar.common.schema.SchemaType.JSON;
import static org.apache.pulsar.common.schema.SchemaType.KEY_VALUE;
import static org.apache.pulsar.common.schema.SchemaType.LOCAL_DATE;
import static org.apache.pulsar.common.schema.SchemaType.LOCAL_DATE_TIME;
import static org.apache.pulsar.common.schema.SchemaType.LOCAL_TIME;
import static org.apache.pulsar.common.schema.SchemaType.NONE;
import static org.apache.pulsar.common.schema.SchemaType.PROTOBUF;
import static org.apache.pulsar.common.schema.SchemaType.PROTOBUF_NATIVE;
import static org.apache.pulsar.common.schema.SchemaType.STRING;
import static org.apache.pulsar.common.schema.SchemaType.TIME;
import static org.apache.pulsar.common.schema.SchemaType.TIMESTAMP;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema hash wrapper with a HashCode inner type.
 */
@EqualsAndHashCode
@Slf4j
public class SchemaHash {

    private static final HashFunction hashFunction = Hashing.sha256();

    private final HashCode hash;

    private final SchemaType schemaType;

    private SchemaHash(HashCode hash, SchemaType schemaType) {
        this.hash = hash;
        this.schemaType = schemaType;
    }

    public static SchemaHash of(Schema schema) {
        Optional<SchemaInfo> schemaInfo = Optional.ofNullable(schema).map(Schema::getSchemaInfo);
        return of(schemaInfo.map(SchemaInfo::getSchema).orElse(null),
                schemaInfo.map(SchemaInfo::getType).orElse(null));
    }

    public static SchemaHash of(SchemaData schemaData) {
        return of(schemaData.getData(), schemaData.getType());
    }

    public static SchemaHash of(SchemaInfo schemaInfo) {
        return of(schemaInfo == null ? null : schemaInfo.getSchema(),
                schemaInfo == null ? null : schemaInfo.getType());
    }

    public static SchemaHash empty() {
        return of(null, null);
    }

    private static SchemaHash of(byte[] schemaBytes, SchemaType schemaType) {
        SchemaHash result;
        if (schemaBytes == null || schemaBytes.length == 0) {
            result = EmptySchemaHashFactory.get(schemaType);
            if (result == null) {
                log.warn("Could not get schemaHash from EmptySchemaHashFactory, will create by hashFunction. Might bring"
                                + " performance regression. schemaBytes:{}, schemaType:{}",
                        schemaBytes == null ? "null" : "0", schemaType);
                result = new SchemaHash(
                        hashFunction.hashBytes(schemaBytes == null ? new byte[0] : schemaBytes), schemaType);
            }
        } else {
            result = new SchemaHash(hashFunction.hashBytes(schemaBytes), schemaType);
        }
        return result;
    }

    public byte[] asBytes() {
        return hash.asBytes();
    }

    private static class EmptySchemaHashFactory {
        private static final HashCode EMPTY_HASH = hashFunction.hashBytes(new byte[0]);
        private static final SchemaHash NONE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, NONE);
        private static final SchemaHash STRING_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, STRING);
        private static final SchemaHash JSON_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, JSON);
        private static final SchemaHash PROTOBUF_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, PROTOBUF);
        private static final SchemaHash AVRO_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, AVRO);
        private static final SchemaHash BOOLEAN_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, BOOLEAN);
        private static final SchemaHash INT8_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, INT8);
        private static final SchemaHash INT16_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, INT16);
        private static final SchemaHash INT32_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, INT32);
        private static final SchemaHash INT64_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, INT64);
        private static final SchemaHash FLOAT_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, FLOAT);
        private static final SchemaHash DOUBLE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, DOUBLE);
        private static final SchemaHash DATE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, DATE);
        private static final SchemaHash TIME_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, TIME);
        private static final SchemaHash TIMESTAMP_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, TIMESTAMP);
        private static final SchemaHash KEY_VALUE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, KEY_VALUE);
        private static final SchemaHash INSTANT_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, INSTANT);
        private static final SchemaHash LOCAL_DATE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, LOCAL_DATE);
        private static final SchemaHash LOCAL_TIME_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, LOCAL_TIME);
        private static final SchemaHash LOCAL_DATE_TIME_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, LOCAL_DATE_TIME);
        private static final SchemaHash PROTOBUF_NATIVE_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, PROTOBUF_NATIVE);
        private static final SchemaHash BYTES_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, BYTES);
        private static final SchemaHash AUTO_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, AUTO);
        private static final SchemaHash AUTO_CONSUME_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, AUTO_CONSUME);
        private static final SchemaHash AUTO_PUBLISH_SCHEMA_HASH = new SchemaHash(EMPTY_HASH, AUTO_PUBLISH);

        public static SchemaHash get(SchemaType schemaType) {
            switch (schemaType) {
                case NONE:
                    return NONE_SCHEMA_HASH;
                case STRING:
                    return STRING_SCHEMA_HASH;
                case JSON:
                    return JSON_SCHEMA_HASH;
                case PROTOBUF:
                    return PROTOBUF_SCHEMA_HASH;
                case AVRO:
                    return AVRO_SCHEMA_HASH;
                case BOOLEAN:
                    return BOOLEAN_SCHEMA_HASH;
                case INT8:
                    return INT8_SCHEMA_HASH;
                case INT16:
                    return INT16_SCHEMA_HASH;
                case INT32:
                    return INT32_SCHEMA_HASH;
                case INT64:
                    return INT64_SCHEMA_HASH;
                case FLOAT:
                    return FLOAT_SCHEMA_HASH;
                case DOUBLE:
                    return DOUBLE_SCHEMA_HASH;
                case DATE:
                    return DATE_SCHEMA_HASH;
                case TIME:
                    return TIME_SCHEMA_HASH;
                case TIMESTAMP:
                    return TIMESTAMP_SCHEMA_HASH;
                case KEY_VALUE:
                    return KEY_VALUE_SCHEMA_HASH;
                case INSTANT:
                    return INSTANT_SCHEMA_HASH;
                case LOCAL_DATE:
                    return LOCAL_DATE_SCHEMA_HASH;
                case LOCAL_TIME:
                    return LOCAL_TIME_SCHEMA_HASH;
                case LOCAL_DATE_TIME:
                    return LOCAL_DATE_TIME_SCHEMA_HASH;
                case PROTOBUF_NATIVE:
                    return PROTOBUF_NATIVE_SCHEMA_HASH;
                case BYTES:
                    return BYTES_SCHEMA_HASH;
                case AUTO:
                    return AUTO_SCHEMA_HASH;
                case AUTO_CONSUME:
                    return AUTO_CONSUME_SCHEMA_HASH;
                case AUTO_PUBLISH:
                    return AUTO_PUBLISH_SCHEMA_HASH;
                default:
                    return null;
            }
        }
    }
}
