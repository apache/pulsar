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
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import java.util.List;

public abstract class AbstractProtobufBaseRecordBuilder implements GenericRecordBuilder {

    private final AbstractGenericProtobufSchema genericSchema;
    private final DynamicMessage.Builder builder;
    private final Descriptors.Descriptor msgDesc;

    public AbstractProtobufBaseRecordBuilder(AbstractGenericProtobufSchema schema) {
        this.genericSchema = schema;
        this.msgDesc = genericSchema.getProtobufBaseSchema();
        builder = DynamicMessage.newBuilder(msgDesc);
    }

    @Override
    public GenericRecordBuilder set(String fieldName, Object value) {
        builder.setField(msgDesc.findFieldByName(fieldName), value);
        return this;
    }

    @Override
    public GenericRecordBuilder set(Field field, Object value) {
        builder.setField(msgDesc.findFieldByName(field.getName()), value);
        return this;
    }

    @Override
    public GenericRecordBuilder clear(String fieldName) {
        builder.clearField(msgDesc.findFieldByName(fieldName));
        return this;
    }

    @Override
    public GenericRecordBuilder clear(Field field) {
        builder.clearField(msgDesc.findFieldByName(field.getName()));
        return this;
    }

    @Override
    public GenericRecord build() {
        return instanceRecord(
                null,
                genericSchema.getProtobufBaseSchema(),
                genericSchema.getFields(),
                builder.build()
        );
    }

    protected abstract GenericRecord instanceRecord(byte[] schemaVersion, Descriptors.Descriptor msgDesc,
                                                    List<Field> fields, DynamicMessage record);
}
