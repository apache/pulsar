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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Util class for processing key/value schema info.
 */
@Slf4j
public final class KeyValueSchemaInfo {

    private static final Schema<SchemaInfo> SCHEMA_INFO_WRITER = new Schema<SchemaInfo>() {
        @Override
        public byte[] encode(SchemaInfo si) {
            return si.getSchema();
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return Schema.BYTES.getSchemaInfo();
        }

        @Override
        public Schema<SchemaInfo> clone() {
            return this;
        }
    };

    private static final String KEY_SCHEMA_NAME = "key.schema.name";
    private static final String KEY_SCHEMA_TYPE = "key.schema.type";
    private static final String KEY_SCHEMA_PROPS = "key.schema.properties";
    private static final String VALUE_SCHEMA_NAME = "value.schema.name";
    private static final String VALUE_SCHEMA_TYPE = "value.schema.type";
    private static final String VALUE_SCHEMA_PROPS = "value.schema.properties";
    private static final String KV_ENCODING_TYPE = "kv.encoding.type";

    /**
     * Decode the kv encoding type from the schema info.
     *
     * @param schemaInfo the schema info
     * @return the kv encoding type
     */
    public static KeyValueEncodingType decodeKeyValueEncodingType(SchemaInfo schemaInfo) {
        checkArgument(SchemaType.KEY_VALUE == schemaInfo.getType(),
            "Not a KeyValue schema");

        String encodingTypeStr = schemaInfo.getProperties().get(KV_ENCODING_TYPE);
        if (StringUtils.isEmpty(encodingTypeStr)) {
            return KeyValueEncodingType.INLINE;
        } else {
            return KeyValueEncodingType.valueOf(encodingTypeStr);
        }
    }

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param keySchema the key schema
     * @param valueSchema the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    public static <K, V> SchemaInfo encodeKeyValueSchemaInfo(Schema<K> keySchema,
                                                             Schema<V> valueSchema,
                                                             KeyValueEncodingType keyValueEncodingType) {
        return encodeKeyValueSchemaInfo(
            "KeyValue",
            keySchema,
            valueSchema,
            keyValueEncodingType
        );
    }

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param schemaName the final schema name
     * @param keySchema the key schema
     * @param valueSchema the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    public static <K, V> SchemaInfo encodeKeyValueSchemaInfo(String schemaName,
                                                             Schema<K> keySchema,
                                                             Schema<V> valueSchema,
                                                             KeyValueEncodingType keyValueEncodingType) {
        return encodeKeyValueSchemaInfo(
            schemaName,
            keySchema.getSchemaInfo(),
            valueSchema.getSchemaInfo(),
            keyValueEncodingType
        );
    }

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param schemaName the final schema name
     * @param keySchemaInfo the key schema info
     * @param valueSchemaInfo the value schema info
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    public static SchemaInfo encodeKeyValueSchemaInfo(String schemaName,
                                                      SchemaInfo keySchemaInfo,
                                                      SchemaInfo valueSchemaInfo,
                                                      KeyValueEncodingType keyValueEncodingType) {
        checkNotNull(keyValueEncodingType, "Null encoding type is provided");

        if (keySchemaInfo == null || valueSchemaInfo == null) {
            // schema is not ready
            return null;
        }

        // process key/value schema data
        byte[] schemaData = KeyValue.encode(
            keySchemaInfo,
            SCHEMA_INFO_WRITER,
            valueSchemaInfo,
            SCHEMA_INFO_WRITER
        );

        // process key/value schema properties
        Map<String, String> properties = new HashMap<>();
        encodeSubSchemaInfoToParentSchemaProperties(
            keySchemaInfo,
            KEY_SCHEMA_NAME,
            KEY_SCHEMA_TYPE,
            KEY_SCHEMA_PROPS,
            properties
        );

        encodeSubSchemaInfoToParentSchemaProperties(
            valueSchemaInfo,
            VALUE_SCHEMA_NAME,
            VALUE_SCHEMA_TYPE,
            VALUE_SCHEMA_PROPS,
            properties
        );
        properties.put(KV_ENCODING_TYPE, String.valueOf(keyValueEncodingType));

        // generate the final schema info
        return SchemaInfoImpl.builder()
                .name(schemaName)
                .type(SchemaType.KEY_VALUE)
                .schema(schemaData)
                .properties(properties)
                .build();
    }

    private static void encodeSubSchemaInfoToParentSchemaProperties(SchemaInfo schemaInfo,
                                                                    String schemaNameProperty,
                                                                    String schemaTypeProperty,
                                                                    String schemaPropsProperty,
                                                                    Map<String, String> parentSchemaProperties) {
        parentSchemaProperties.put(schemaNameProperty, schemaInfo.getName());
        parentSchemaProperties.put(schemaTypeProperty, String.valueOf(schemaInfo.getType()));
        parentSchemaProperties.put(
            schemaPropsProperty,
            SchemaUtils.serializeSchemaProperties(schemaInfo.getProperties()));
    }

    /**
     * Decode the key/value schema info to get key schema info and value schema info.
     *
     * @param schemaInfo key/value schema info.
     * @return the pair of key schema info and value schema info
     */
    public static KeyValue<SchemaInfo, SchemaInfo> decodeKeyValueSchemaInfo(SchemaInfo schemaInfo) {
        checkArgument(SchemaType.KEY_VALUE == schemaInfo.getType(),
            "Not a KeyValue schema");

        return KeyValue.decode(
            schemaInfo.getSchema(),
            (keyBytes, valueBytes) -> {
                SchemaInfo keySchemaInfo = decodeSubSchemaInfo(
                    schemaInfo,
                    KEY_SCHEMA_NAME,
                    KEY_SCHEMA_TYPE,
                    KEY_SCHEMA_PROPS,
                    keyBytes
                );

                SchemaInfo valueSchemaInfo = decodeSubSchemaInfo(
                    schemaInfo,
                    VALUE_SCHEMA_NAME,
                    VALUE_SCHEMA_TYPE,
                    VALUE_SCHEMA_PROPS,
                    valueBytes
                );
                return new KeyValue<>(keySchemaInfo, valueSchemaInfo);
            }
        );
    }

    private static SchemaInfo decodeSubSchemaInfo(SchemaInfo parentSchemaInfo,
                                                  String schemaNameProperty,
                                                  String schemaTypeProperty,
                                                  String schemaPropsProperty,
                                                  byte[] schemaData) {
        Map<String, String> parentSchemaProps = parentSchemaInfo.getProperties();
        String schemaName = parentSchemaProps.getOrDefault(schemaNameProperty, "");
        SchemaType schemaType =
            SchemaType.valueOf(parentSchemaProps.getOrDefault(schemaTypeProperty, SchemaType.BYTES.name()));
        Map<String, String> schemaProps;
        String schemaPropsStr = parentSchemaProps.get(schemaPropsProperty);
        if (StringUtils.isEmpty(schemaPropsStr)) {
            schemaProps = Collections.emptyMap();
        } else {
            schemaProps = SchemaUtils.deserializeSchemaProperties(schemaPropsStr);
        }
        return SchemaInfoImpl.builder()
            .name(schemaName)
            .type(schemaType)
            .schema(schemaData)
            .properties(schemaProps)
            .build();
    }

    private KeyValueSchemaInfo() {}
}
