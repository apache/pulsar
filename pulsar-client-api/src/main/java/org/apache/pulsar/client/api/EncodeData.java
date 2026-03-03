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
package org.apache.pulsar.client.api;

/**
 * Data to be encoded by an external schema.
 *
 * @param data the message payload to be encoded
 * @param schemaId the schema id return by the schema registry, it can be null if not applicable
 */
public record EncodeData(byte[] data, byte[] schemaId) {

    public EncodeData(byte[] data) {
        this(data, null);
    }

    public boolean hasSchemaId() {
        return isValidSchemaId(schemaId);
    }

    public static boolean isValidSchemaId(byte[] schemaId) {
        return schemaId != null && schemaId.length > 0;
    }

}
