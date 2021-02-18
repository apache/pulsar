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
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.Field;

public class GenericProtobufNativeField extends Field {

    private final Descriptors.FieldDescriptor fieldDescriptor;

    public GenericProtobufNativeField(Descriptors.FieldDescriptor f) {
        super(f.getName(), f.getIndex());
        this.fieldDescriptor = f;
    }

    @Override
    public <T> T unwrap(Class<T> api) {
        if (api == Descriptors.FieldDescriptor.class) {
            return (T) fieldDescriptor;
        }
        return super.unwrap(api);
    }
}
