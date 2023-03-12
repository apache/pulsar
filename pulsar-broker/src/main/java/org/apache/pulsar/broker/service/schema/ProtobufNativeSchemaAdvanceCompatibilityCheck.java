/*
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.protobuf.Descriptors.Descriptor;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.ProtoBufCanReadCheckException;
import org.apache.pulsar.broker.service.schema.validator.ProtobufNativeSchemaBreakValidator;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The {@link SchemaCompatibilityCheck} implementation for {@link SchemaType#PROTOBUF_NATIVE}.
 * Better than {@link ProtobufNativeSchemaCompatibilityCheck} compatibility check rules.
 */
public class ProtobufNativeSchemaAdvanceCompatibilityCheck implements SchemaCompatibilityCheck {

    @Override
    public SchemaType getSchemaType() {
        return SchemaType.PROTOBUF_NATIVE;
    }

    @Override
    public void checkCompatible(SchemaData from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkCompatible(Collections.singletonList(from), to, strategy);
    }

    @Override
    public void checkCompatible(Iterable<SchemaData> from, SchemaData to, SchemaCompatibilityStrategy strategy)
            throws IncompatibleSchemaException {
        checkArgument(from != null, "check compatibility list is null");
        LinkedList<Descriptor> existingSchemasList = new LinkedList<>();
        try {
            for (SchemaData schemaData : from) {
                existingSchemasList.addFirst(ProtobufNativeSchemaUtils.deserialize(schemaData.getData()));
            }
            Iterator<Descriptor> existingSchemas = existingSchemasList.iterator();
            Descriptor newSchema = ProtobufNativeSchemaUtils.deserialize(to.getData());
            ProtobufNativeSchemaBreakValidator protobufNativeSchemaBreakValidator =
                    new ProtobufNativeSchemaBreakValidator();

            switch (strategy) {
                case BACKWARD_TRANSITIVE -> {
                    for (Descriptor existingSchema : existingSchemasList) {
                        protobufNativeSchemaBreakValidator.canRead(existingSchema, newSchema);
                    }
                }
                case BACKWARD -> {
                    if (existingSchemas.hasNext()) {
                        Descriptor existingSchema = existingSchemas.next();
                        protobufNativeSchemaBreakValidator.canRead(existingSchema, newSchema);
                    }
                }
                case FORWARD_TRANSITIVE -> {
                    for (Descriptor existingSchema : existingSchemasList) {
                        protobufNativeSchemaBreakValidator.canRead(newSchema, existingSchema);
                    }
                }
                case FORWARD -> {
                    if (existingSchemas.hasNext()) {
                        Descriptor existingSchema = existingSchemas.next();
                        protobufNativeSchemaBreakValidator.canRead(newSchema, existingSchema);
                    }
                }
                case FULL_TRANSITIVE -> {
                    for (Descriptor existingSchema : existingSchemasList) {
                        protobufNativeSchemaBreakValidator.canRead(existingSchema, newSchema);
                        protobufNativeSchemaBreakValidator.canRead(newSchema, existingSchema);
                    }
                }
                case FULL -> {
                    if (existingSchemas.hasNext()) {
                        Descriptor existingSchema = existingSchemas.next();
                        protobufNativeSchemaBreakValidator.canRead(existingSchema, newSchema);
                        protobufNativeSchemaBreakValidator.canRead(newSchema, existingSchema);
                    }
                }
                case ALWAYS_COMPATIBLE -> {
                    return;
                }
                default -> throw new ProtoBufCanReadCheckException("Unknown SchemaCompatibilityStrategy.");
            }
        } catch (ProtoBufCanReadCheckException e) {
            throw new IncompatibleSchemaException(e);
        }
    }

}
