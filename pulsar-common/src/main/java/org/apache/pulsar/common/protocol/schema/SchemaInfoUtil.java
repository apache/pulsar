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

import java.util.Collections;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.Schema;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Class helping to initialize schemas.
 */
@UtilityClass
public class SchemaInfoUtil {

    public static SchemaInfo newSchemaInfo(String name, SchemaData data) {
        SchemaInfo si = new SchemaInfo();
        si.setName(name);
        si.setSchema(data.getData());
        si.setType(data.getType());
        si.setProperties(data.getProps());
        return si;
    }

    public static SchemaInfo newSchemaInfo(Schema schema) {
        SchemaInfo si = new SchemaInfo();
        si.setName(schema.getName());
        si.setSchema(schema.getSchemaData().toByteArray());
        si.setType(Commands.getSchemaType(schema.getType()));
        if (schema.getPropertiesCount() == 0) {
            si.setProperties(Collections.emptyMap());
        } else {
            si.setProperties(new TreeMap<>());
            for (int i = 0; i < schema.getPropertiesCount(); i++) {
                PulsarApi.KeyValue kv = schema.getProperties(i);
                si.getProperties().put(kv.getKey(), kv.getValue());
            }
        }
        return si;
    }

    public static SchemaInfo newSchemaInfo(String name, GetSchemaResponse schema) {
        SchemaInfo si = new SchemaInfo();
        si.setName(name);
        si.setSchema(schema.getData().getBytes());
        si.setType(schema.getType());
        si.setProperties(schema.getProperties());
        return si;
    }
}
