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
package org.apache.pulsar.io.kafka;

import java.nio.ByteBuffer;
import lombok.Value;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * This is a wrapper around a Byte array (the Avro/Protobuf encoded record), a schema id in the Kafka Schema Registry
 * and the schemaType(one of Avro and Protobuf_Native).
 */
@Value
public class BytesWithKafkaSchema {
    ByteBuffer value;
    int schemaId;
    SchemaType schemaType;
}
