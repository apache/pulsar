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
package org.apache.pulsar.broker.service.schema.validator;

import com.google.protobuf.Descriptors;
import org.apache.pulsar.broker.service.schema.exceptions.InvalidSchemaDataException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.protocol.schema.SchemaData;

public class ProtobufNativeSchemaDataValidator implements SchemaDataValidator {

    @Override
    public void validate(SchemaData schemaData) throws InvalidSchemaDataException {
        Descriptors.Descriptor descriptor;
        try {
            descriptor = ProtobufNativeSchemaUtils.deserialize(schemaData.getData());
        } catch (Exception e) {
            throw new InvalidSchemaDataException("deserialize ProtobufNative Schema failed", e);
        }
        if (descriptor == null) {
            throw new InvalidSchemaDataException(
                    "protobuf root message descriptor is null,"
                            + " please recheck rootMessageTypeName or rootFileDescriptorName conf. ");
        }
    }

    public static ProtobufNativeSchemaDataValidator of() {
        return INSTANCE;
    }

    private static final ProtobufNativeSchemaDataValidator INSTANCE = new ProtobufNativeSchemaDataValidator();

    private ProtobufNativeSchemaDataValidator() {
    }
}
