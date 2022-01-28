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
package org.apache.pulsar.broker.service.schema;

import static com.google.protobuf.Descriptors.Descriptor;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The {@link SchemaCompatibilityCheck} implementation for {@link SchemaType#PROTOBUF_NATIVE}.
 */
public class ProtobufNativeSchemaCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        Descriptor fromDescriptor = ProtobufNativeSchemaUtils.deserialize(from.getData());
        Descriptor toDescriptor = ProtobufNativeSchemaUtils.deserialize(to.getData());
        switch (strategy) {
            case BACKWARD_TRANSITIVE:
            case BACKWARD:
            case FORWARD_TRANSITIVE:
            case FORWARD:
            case FULL_TRANSITIVE:
            case FULL:
                checkRootMessageChange(fromDescriptor, toDescriptor, strategy);
                return;
            case ALWAYS_COMPATIBLE:
                return;
            default:
                throw new IncompatibleSchemaException("Unknown SchemaCompatibilityStrategy.");
        }
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        for (SchemaData schemaData : from) {
            checkCompatible(schemaData, to, strategy);
        }
    }

    private void checkRootMessageChange(Descriptor fromDescriptor, Descriptor toDescriptor,
                                            SchemaCompatibilityStrategy strategy) throws IncompatibleSchemaException {
        if (!fromDescriptor.getFullName().equals(toDescriptor.getFullName())) {
            throw new IncompatibleSchemaException("Protobuf root message isn't allow change!");
        }
    }

}
