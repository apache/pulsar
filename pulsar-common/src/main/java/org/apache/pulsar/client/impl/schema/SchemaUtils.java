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

import static java.nio.charset.StandardCharsets.UTF_8;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Utils for schemas.
 */
public final class SchemaUtils {

    private static final byte[] KEY_VALUE_SCHEMA_IS_PRIMITIVE = new byte[0];

    private static final String KEY_VALUE_SCHEMA_NULL_STRING = "\"\"";

    private SchemaUtils() {}

    /**
     * Keeps a map between {@link SchemaType} to a list of java classes that can be used to represent them.
     */
    private static final Map<SchemaType, List<Class>> SCHEMA_TYPE_CLASSES = new HashMap<>();

    /**
     * Maps the java classes to the corresponding {@link SchemaType}.
     */
    private static final Map<Class<?>, SchemaType> JAVA_CLASS_SCHEMA_TYPES = new HashMap<>();

    static {
        // int8
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT8,
                Arrays.asList(Byte.class));
        // int16
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT16,
                Arrays.asList(Short.class));
        // int32
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT32,
                Arrays.asList(Integer.class));
        // int64
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.INT64,
                Arrays.asList(Long.class));
        // float
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.FLOAT,
                Arrays.asList(Float.class));
        // double
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.DOUBLE,
                Arrays.asList(Double.class));
        // boolean
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.BOOLEAN,
                Arrays.asList(Boolean.class));
        // string
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.STRING,
                Arrays.asList(String.class));
        // bytes
        SCHEMA_TYPE_CLASSES.put(
                SchemaType.BYTES,
                Arrays.asList(byte[].class, ByteBuffer.class, ByteBuf.class));
        // build the reverse mapping
        SCHEMA_TYPE_CLASSES.forEach(
                (type, classes) -> classes.forEach(clz -> JAVA_CLASS_SCHEMA_TYPES.put(clz, type)));
    }

    public static void validateFieldSchema(String name,
                                           SchemaType type,
                                           Object val) {
        if (null == val) {
            return;
        }

        List<Class> expectedClasses = SCHEMA_TYPE_CLASSES.get(type);

        if (null == expectedClasses) {
            throw new RuntimeException("Invalid Java object for schema type " + type
                    + " : " + val.getClass()
                    + " for field : \"" + name + "\"");
        }

        boolean foundMatch = false;
        for (Class<?> expectedCls : expectedClasses) {
            if (expectedCls.isInstance(val)) {
                foundMatch = true;
                break;
            }
        }

        if (!foundMatch) {
            throw new RuntimeException("Invalid Java object for schema type " + type
                    + " : " + val.getClass()
                    + " for field : \"" + name + "\"");
        }

        switch (type) {
            case INT8:
            case INT16:
            case PROTOBUF:
            case AVRO:
            case AUTO_CONSUME:
            case AUTO_PUBLISH:
            case AUTO:
            case KEY_VALUE:
            case JSON:
            case NONE:
                throw new RuntimeException("Currently " + type.name() + " is not supported");
            default:
                break;
        }
    }

    public static Object toAvroObject(Object value) {
        if (value != null) {
            if (value instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) value;
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                return bytes;
            } else if (value instanceof ByteBuf) {
                return ByteBufUtil.getBytes((ByteBuf) value);
            } else {
                return value;
            }
        } else {
            return null;
        }
    }

    public static String getStringSchemaVersion(byte[] schemaVersionBytes) {
        if (null == schemaVersionBytes) {
            return "NULL";
        } else if (
            // the length of schema version is 8 bytes post 2.4.0
            schemaVersionBytes.length == Long.BYTES
            // the length of schema version is 64 bytes before 2.4.0
            || schemaVersionBytes.length == Long.SIZE) {
            ByteBuffer bb = ByteBuffer.wrap(schemaVersionBytes);
            return String.valueOf(bb.getLong());
        } else if (schemaVersionBytes.length == 0) {
            return "EMPTY";
        } else {
            return Base64.getEncoder().encodeToString(schemaVersionBytes);
        }

    }

    /**
     * Jsonify the schema info.
     *
     * @param schemaInfo the schema info
     * @return the jsonified schema info
     */
    public static String jsonifySchemaInfo(SchemaInfo schemaInfo) {
        GsonBuilder gsonBuilder = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToStringAdapter(schemaInfo))
            .registerTypeHierarchyAdapter(Map.class, SCHEMA_PROPERTIES_SERIALIZER);

        return gsonBuilder.create().toJson(schemaInfo);
    }

    /**
     * Jsonify the schema info with verison.
     *
     * @param schemaInfoWithVersion the schema info
     * @return the jsonified schema info with version
     */
    public static String jsonifySchemaInfoWithVersion(SchemaInfoWithVersion schemaInfoWithVersion) {
        GsonBuilder gsonBuilder = new GsonBuilder()
                .setPrettyPrinting()
                .registerTypeHierarchyAdapter(SchemaInfo.class, SCHEMAINFO_ADAPTER)
                .registerTypeHierarchyAdapter(Map.class, SCHEMA_PROPERTIES_SERIALIZER);

        return gsonBuilder.create().toJson(schemaInfoWithVersion);
    }

    private static class SchemaPropertiesSerializer implements JsonSerializer<Map<String, String>> {

        @Override
        public JsonElement serialize(Map<String, String> properties,
                                     Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            SortedMap<String, String> sortedProperties = new TreeMap<>();
            sortedProperties.putAll(properties);
            JsonObject object = new JsonObject();
            sortedProperties.forEach((key, value) -> {
                object.add(key, (value != null) ? new JsonPrimitive(value) : null);
            });
            return object;
        }

    }

    private static class SchemaPropertiesDeserializer implements JsonDeserializer<Map<String, String>> {

        @Override
        public Map<String, String> deserialize(JsonElement jsonElement,
                                               Type type,
                                               JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {

            SortedMap<String, String> sortedProperties = new TreeMap<>();
            jsonElement.getAsJsonObject().entrySet().forEach(entry -> sortedProperties.put(
                entry.getKey(),
                entry.getValue().getAsString()
            ));
            return sortedProperties;
        }

    }

    private static final SchemaPropertiesSerializer SCHEMA_PROPERTIES_SERIALIZER =
        new SchemaPropertiesSerializer();

    private static final SchemaPropertiesDeserializer SCHEMA_PROPERTIES_DESERIALIZER =
        new SchemaPropertiesDeserializer();

    private static class ByteArrayToStringAdapter implements JsonSerializer<byte[]> {

        private final SchemaInfo schemaInfo;

        public ByteArrayToStringAdapter(SchemaInfo schemaInfo) {
            this.schemaInfo = schemaInfo;
        }

        public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
            String schemaDef = schemaInfo.getSchemaDefinition();
            SchemaType type = schemaInfo.getType();
            switch (type) {
                case AVRO:
                case JSON:
                case PROTOBUF:
                    return toJsonElement(schemaInfo.getSchemaDefinition());
                case KEY_VALUE:
                    KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue =
                        DefaultImplementation.getDefaultImplementation().decodeKeyValueSchemaInfo(schemaInfo);
                    JsonObject obj = new JsonObject();
                    String keyJson = jsonifySchemaInfo(schemaInfoKeyValue.getKey());
                    String valueJson = jsonifySchemaInfo(schemaInfoKeyValue.getValue());
                    obj.add("key", toJsonElement(keyJson));
                    obj.add("value", toJsonElement(valueJson));
                    return obj;
                default:
                    return new JsonPrimitive(schemaDef);
            }
        }
    }

    public static JsonElement toJsonElement(String str) {
        try {
            return JsonParser.parseString(str).getAsJsonObject();
        } catch (IllegalStateException e) {
            // because str may not a json, so we should use JsonPrimitive
            return new JsonPrimitive(str);
        }
    }

    private static class SchemaInfoToStringAdapter implements JsonSerializer<SchemaInfo> {

        @Override
        public JsonElement serialize(SchemaInfo schemaInfo,
                                     Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            // schema will not a json, so use toJsonElement
            return toJsonElement(jsonifySchemaInfo(schemaInfo));
        }
    }

    private static final SchemaInfoToStringAdapter SCHEMAINFO_ADAPTER = new SchemaInfoToStringAdapter();

    /**
     * Jsonify the key/value schema info.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the jsonified schema info
     */
    public static String jsonifyKeyValueSchemaInfo(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) {
        GsonBuilder gsonBuilder = new GsonBuilder()
            .registerTypeHierarchyAdapter(SchemaInfo.class, SCHEMAINFO_ADAPTER)
            .registerTypeHierarchyAdapter(Map.class, SCHEMA_PROPERTIES_SERIALIZER);
        return gsonBuilder.create().toJson(kvSchemaInfo);
    }

    /**
     * Convert the key/value schema info data to string.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the convert schema info data string
     */
    public static String convertKeyValueSchemaInfoDataToString(
            KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) throws IOException {
        ObjectMapper objectMapper = ObjectMapperFactory.create();
        KeyValue<Object, Object> keyValue = new KeyValue<>(
                SchemaType.isPrimitiveType(kvSchemaInfo.getKey().getType()) ? ""
                        : objectMapper.readTree(kvSchemaInfo.getKey().getSchema()),
                SchemaType.isPrimitiveType(kvSchemaInfo.getValue().getType()) ? ""
                        : objectMapper.readTree(kvSchemaInfo.getValue().getSchema()));
        return objectMapper.writeValueAsString(keyValue);
    }

    private static byte[] getKeyOrValueSchemaBytes(JsonElement jsonElement) {
        return KEY_VALUE_SCHEMA_NULL_STRING.equals(jsonElement.toString())
                ? KEY_VALUE_SCHEMA_IS_PRIMITIVE : jsonElement.toString().getBytes(UTF_8);
    }

    /**
     * Convert the key/value schema info data json bytes to key/value schema info data bytes.
     *
     * @param keyValueSchemaInfoDataJsonBytes the key/value schema info data json bytes
     * @return the key/value schema info data bytes
     */
    public static byte[] convertKeyValueDataStringToSchemaInfoSchema(
            byte[] keyValueSchemaInfoDataJsonBytes) throws IOException {
        JsonObject jsonObject = (JsonObject) toJsonElement(new String(keyValueSchemaInfoDataJsonBytes, UTF_8));
        byte[] keyBytes = getKeyOrValueSchemaBytes(jsonObject.get("key"));
        byte[] valueBytes = getKeyOrValueSchemaBytes(jsonObject.get("value"));
        int dataLength = 4 + keyBytes.length + 4 + valueBytes.length;
        byte[] schema = new byte[dataLength];
        //record the key value schema respective length
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.heapBuffer(dataLength);
        byteBuf.writeInt(keyBytes.length).writeBytes(keyBytes).writeInt(valueBytes.length).writeBytes(valueBytes);
        byteBuf.readBytes(schema);
        return schema;
    }

    /**
     * Serialize schema properties.
     *
     * @param properties schema properties
     * @return the serialized schema properties
     */
    public static String serializeSchemaProperties(Map<String, String> properties) {
        GsonBuilder gsonBuilder = new GsonBuilder()
            .registerTypeHierarchyAdapter(Map.class, SCHEMA_PROPERTIES_SERIALIZER);
        return gsonBuilder.create().toJson(properties);
    }

    /**
     * Deserialize schema properties from a serialized schema properties.
     *
     * @param serializedProperties serialized properties
     * @return the deserialized properties
     */
    public static Map<String, String> deserializeSchemaProperties(String serializedProperties) {
        GsonBuilder gsonBuilder = new GsonBuilder()
            .registerTypeHierarchyAdapter(Map.class, SCHEMA_PROPERTIES_DESERIALIZER);
        return gsonBuilder.create().fromJson(serializedProperties, Map.class);
    }

}
