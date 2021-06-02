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

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;

import java.util.Objects;

public abstract class AbstractSchema<T> implements Schema<T> {

    /**
     * Check if the message read able length length is a valid object for this schema.
     *
     * <p>The implementation can choose what its most efficient approach to validate the schema.
     * If the implementation doesn't provide it, it will attempt to use {@link #decode(ByteBuf)}
     * to see if this schema can decode this message or not as a validation mechanism to verify
     * the bytes.
     *
     * @param byteBuf the messages to verify
     * @return true if it is a valid message
     * @throws SchemaSerializationException if it is not a valid message
     */
    void validate(ByteBuf byteBuf) {
        throw new SchemaSerializationException("This method is not supported");
    };

    /**
     * Decode a byteBuf into an object using the schema definition and deserializer implementation
     * <p>Do not modify reader/writer index of ByteBuf so, it can be reused to access correct data.
     *
     * @param byteBuf
     *            the byte buffer to decode
     * @return the deserialized object
     */
    public abstract T decode(ByteBuf byteBuf);
    /**
     * Decode a byteBuf into an object using a given version.
     *
     * @param byteBuf
     *            the byte array to decode
     * @param schemaVersion
     *            the schema version to decode the object. null indicates using latest version.
     * @return the deserialized object
     */
    public T decode(ByteBuf byteBuf, byte[] schemaVersion) {
        // ignore version by default (most of the primitive schema implementations ignore schema version)
        return decode(byteBuf);
    }
    
    @Override
    public Schema<T> clone() {
        return this;
    }

    /**
     * Return an instance of this schema at the given version.
     * @param schemaVersion the version
     * @return the schema at that specific version
     * @throws SchemaSerializationException in case of unknown schema version
     * @throws NullPointerException in case of null schemaVersion
     */
    public Schema<?> atSchemaVersion(byte[] schemaVersion) throws SchemaSerializationException {
        Objects.requireNonNull(schemaVersion);
        if (!supportSchemaVersioning()) {
            return this;
        } else {
            throw new SchemaSerializationException("Not implemented for " + this.getClass());
        }
    }
}
