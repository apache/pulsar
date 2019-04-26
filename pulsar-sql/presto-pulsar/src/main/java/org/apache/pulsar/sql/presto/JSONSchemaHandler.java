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
package org.apache.pulsar.sql.presto;

import com.dslplatform.json.DslJson;
import com.facebook.presto.spi.type.Type;

import io.airlift.log.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.avro.Schema;

public class JSONSchemaHandler implements SchemaHandler {

    private static final Logger log = Logger.get(JSONSchemaHandler.class);

    private List<PulsarColumnHandle> columnHandles;

    private final DslJson<Object> dslJson = new DslJson<>();

    private static final FastThreadLocal<byte[]> tmpBuffer = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[1024];
        }
    };
    private Schema schema;
    private Class objectClass;

    public JSONSchemaHandler(Schema schema, List<PulsarColumnHandle> columnHandles) {
        this.schema = schema;
        switch (schema.getType()) {
            case RECORD:
                objectClass = Map.class;
                break;
            case ENUM:
                objectClass = Enum.class;
                break;
            case ARRAY:
                objectClass = Collection.class;
                break;
            case MAP:
                objectClass = Map.class;
                break;
            case UNION:
                objectClass = Map.class;
                break;
            case STRING:
                objectClass = String.class;
                break;
            case BYTES:
                objectClass = ByteBuffer.class;
                break;
            case INT:
                objectClass = Integer.class;
                break;
            case LONG:
                objectClass = Long.class;
                break;
            case FLOAT:
                objectClass = Float.class;
                break;
            case DOUBLE:
                objectClass = Double.class;
                break;
            case BOOLEAN:
                objectClass = Boolean.class;
                break;
            case NULL:
                objectClass = Void.class;
                break;
            default:
                objectClass = Map.class;
        }
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(ByteBuf payload) {
        // Since JSON deserializer only works on a byte[] we need to convert a direct mem buffer into
        // a byte[].
        int size = payload.readableBytes();
        byte[] buffer = tmpBuffer.get();
        if (buffer.length < size) {
            // If the thread-local buffer is not big enough, replace it with
            // a bigger one
            buffer = new byte[size * 2];
            tmpBuffer.set(buffer);
        }

        payload.readBytes(buffer, 0, size);

        try {
            return dslJson.deserialize(objectClass, buffer, size);
        } catch (IOException e) {
            log.error("Failed to deserialize Json object", e);
            return null;
        }
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        if (PulsarMetadata.isPrimitiveType(schema.getType())) {
            return currentRecord;
        }
        try {
            Map jsonObject = (Map) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = columnHandles.get(index);

            String[] fieldNames = pulsarColumnHandle.getFieldNames();
            Object field = jsonObject.get(fieldNames[0]);
            if (field == null) {
                return null;
            }
            for (int i = 1; i < fieldNames.length ; i++) {
                field = ((Map) field).get(fieldNames[i]);
                if (field == null) {
                    return null;
                }
            }

            Type type = pulsarColumnHandle.getType();

            Class<?> javaType = type.getJavaType();

            if (javaType == double.class) {
                return ((BigDecimal) field).doubleValue();
            }

            return field;
        } catch (Exception ex) {
            log.debug(ex,"%s", ex);
        }
        return null;
    }
}
