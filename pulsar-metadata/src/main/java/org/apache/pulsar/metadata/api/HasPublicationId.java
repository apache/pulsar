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
package org.apache.pulsar.metadata.api;

/** A metadata record that persists a unique ID for each logical write and reuses it on retries. */
public interface HasPublicationId {
    /**
     * Returns the publication ID stored in this record, or null or empty if absent.
     *
     * <p>Confirming a write after a bad-version response through this getter would require deserializing the stored
     * record. Since the publication ID is included in the serialized bytes, the metadata store can compare those
     * bytes directly, together with the expected next version, to avoid that CPU overhead. Verification therefore
     * does not need to call this getter.
     *
     * <p>This getter and {@link #setPublication(String)} define a readable and writable publication property for
     * implementing records. Implementations must serialize this property as part of the record. Each logical write
     * must use a unique publication ID and preserve it across retries of that write.
     */
    String getPublication();

    /**
     * Sets the publication ID to include in the serialized record.
     *
     * @param publication the unique ID for this logical write, reused across its retries
     * @return this record
     */
    HasPublicationId setPublication(String publication);
}
