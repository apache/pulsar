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
package org.apache.pulsar.metadata.api;

import java.io.IOException;

/**
 * Interface that define a serializer/deserializer implementation.
 *
 * @param <T>
 */
public interface MetadataSerde<T> {

    /**
     * Serialize the object into a byte array.
     *
     * @param path the path of the object on the metadata store
     * @param value the object instance
     * @return a byte array of the serialized version
     * @throws IOException if the serialization fails
     */
    byte[] serialize(String path, T value) throws IOException;

    /**
     * Serialize the object from a byte array.
     * @param path the path of the object on the metadata store
     * @param content the content as stored on metadata store
     * @param stat the {@link Stat} metadata for the object
     * @return the deserialized object
     * @throws IOException if the deserialization fails
     */
    T deserialize(String path, byte[] content, Stat stat) throws IOException;
}
