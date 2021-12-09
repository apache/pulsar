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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.schema.ProtobufSchemaUtils;
import org.apache.pulsar.common.schema.SchemaInfo;

@Slf4j
public class MultiVersionGenericProtobufReader extends AbstractMultiVersionGenericProtobufReader {


    public MultiVersionGenericProtobufReader(boolean useProvidedSchemaAsReaderSchema, SchemaInfo schemaInfo) {
        super(useProvidedSchemaAsReaderSchema, schemaInfo,
                new GenericProtobufReader(
                        ProtobufSchemaUtils.parseAvroBaseSchemaInfoToProtobufDescriptor(schemaInfo.getSchema())));
    }

    @Override
    protected Descriptors.Descriptor parserSchemaInfoToDescriptor(SchemaInfo schemaInfo) {
        return ProtobufSchemaUtils.parseAvroBaseSchemaInfoToProtobufDescriptor(schemaInfo.getSchema());
    }

    @Override
    protected AbstractGenericProtobufReader instanceReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        return new GenericProtobufReader(descriptor, schemaVersion);
    }
}
