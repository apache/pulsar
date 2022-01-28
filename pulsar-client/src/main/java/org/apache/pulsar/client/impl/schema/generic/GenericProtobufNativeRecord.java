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
package org.apache.pulsar.client.impl.schema.generic;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.common.schema.SchemaType;

public class GenericProtobufNativeRecord extends VersionedGenericRecord {

    private final DynamicMessage record;
    private final Descriptors.Descriptor msgDesc;

    protected GenericProtobufNativeRecord(byte[] schemaVersion,
                                          Descriptors.Descriptor msgDesc,
                                          List<Field> fields,
                                          DynamicMessage record) {
        super(schemaVersion, fields);
        this.msgDesc = msgDesc;
        this.record = record;
    }

    @Override
    public Object getField(String fieldName) {
        return record.getField(msgDesc.findFieldByName(fieldName));
    }

    public DynamicMessage getProtobufRecord() {
        return record;
    }

    @Override
    public Object getNativeObject() {
        return record;
    }

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }
}
