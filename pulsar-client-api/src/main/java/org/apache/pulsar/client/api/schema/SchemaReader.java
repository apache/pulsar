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
package org.apache.pulsar.client.api.schema;

import java.io.InputStream;
import java.util.Optional;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Deserialize messages from bytes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface SchemaReader<T> {

    /**
     * Serialize bytes convert pojo.
     *
     * @param bytes the data
     * @return the serialized object
     */
    default T read(byte[] bytes) {
        return read(bytes, 0, bytes.length);
    }

    /**
     * serialize bytes convert pojo.
     *
     * @param bytes the data
     * @param offset the byte[] initial position
     * @param length the byte[] read length
     * @return the serialized object
     */
    T read(byte[] bytes, int offset, int length);

    /**
     * serialize bytes convert pojo.
     *
     * @param inputStream the stream of message
     * @return the serialized object
     */
    T read(InputStream inputStream);

    /**
     * Serialize bytes convert pojo.
     *
     * @param bytes the data
     * @param schemaVersion the schema version of message
     * @return the serialized object
     */
    default T read(byte[] bytes, byte[] schemaVersion) {
        return read(bytes, 0, bytes.length);
    }

    /**
     * serialize bytes convert pojo.
     *
     * @param inputStream the stream of message
     * @param schemaVersion the schema version of message
     * @return the serialized object
     */
    default T read(InputStream inputStream, byte[] schemaVersion) {
        return read(inputStream);
    }

    /**
     * Set schema info provider, this method support multi version reader.
     *
     * @param schemaInfoProvider the stream of message
     */
    default void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
    }

    /**
     * Returns the underling Schema if possible
     * @return the schema, or an empty Optional if it is not possible to access it
     */
    default Optional<Object> getNativeSchema() {
        return Optional.empty();
    }
}
