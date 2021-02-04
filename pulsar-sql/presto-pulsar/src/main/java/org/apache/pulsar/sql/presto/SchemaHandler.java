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
package org.apache.pulsar.sql.presto;

import io.netty.buffer.ByteBuf;

/**
 * This interface defines the methods to work with schemas.
 */
public interface SchemaHandler {

    default Object deserialize(ByteBuf payload) {
        return null;
    }

    // if schemaHandler don't support multi version, we will use method deserialize(ByteBuf payload)
    default Object deserialize(ByteBuf byteBuf, byte[] schemaVersion) {
        return deserialize(byteBuf);
    }

    // if SchemaHandler don't support key value multi version
    // we will use method deserialize(ByteBuf byteBuf, byte[] schemaVersion)
    default Object deserialize(ByteBuf keyPayload, ByteBuf dataPayload, byte[] schemaVersion) {
        return deserialize(dataPayload, schemaVersion);
    }

    Object extractField(int index, Object currentRecord);

}
