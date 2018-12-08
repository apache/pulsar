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
import java.util.List;
import java.util.Map;

public class JSONSchemaHandler implements SchemaHandler {

    private static final Logger log = Logger.get(JSONSchemaHandler.class);

    private List<PulsarColumnHandle> columnHandles;

    private final DslJson<Object> dslJson = new DslJson<>();

    public JSONSchemaHandler(List<PulsarColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(byte[] bytes) {
        try {
            return dslJson.deserialize(Map.class, bytes, bytes.length);
        } catch (IOException e) {
            log.error(e);
        }
        return null;
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
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
