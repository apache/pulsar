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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.parseAvroSchema;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * This is a base schema implementation for Avro Based `Struct` types.
 * A struct type is used for presenting records (objects) which
 * have multiple fields.
 *
 * <p>Currently Pulsar supports 3 `Struct` types -
 * {@link org.apache.pulsar.common.schema.SchemaType#AVRO},
 * {@link org.apache.pulsar.common.schema.SchemaType#JSON},
 * and {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF}.
 */
public abstract class AvroBaseStructSchema<T> extends AbstractStructSchema<T>{

    protected final Schema schema;

    public AvroBaseStructSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        this.schema = parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8));
    }

    public Schema getAvroSchema(){
        return schema;
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(schema);
    }
}
