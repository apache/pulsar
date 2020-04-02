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
package org.apache.pulsar.functions.source;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.SerDe;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SerDeSchema<T> implements Schema<T> {

    private final SerDe<T> serDe;

    @Override
    public byte[] encode(T value) {
        return serDe.serialize(value);
    }

    @Override
    public T decode(byte[] bytes) {
        return serDe.deserialize(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        // Do not persist schema information
        return null;
    }

    @Override
    public Schema<T> clone() {
        return this;
    }
}
