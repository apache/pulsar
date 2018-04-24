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
package org.apache.pulsar.client.api.schemas;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.nio.ByteBuffer;

public class IntegerSchema implements Schema<Integer> {
    public IntegerSchema() {}

    public byte[] encode(Integer i) {
        return ByteBuffer.allocate(4).putInt(i).array();
    }

    public Integer decode(byte[] data) {
        return ((Byte) data[0]).intValue();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setName("Integer");
        schemaInfo.setType(SchemaType.NONE);
        return schemaInfo;
    }
}
