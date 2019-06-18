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
package org.apache.pulsar.client.admin;

import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Admin interface on interacting with schemas.
 */
public interface Schemas {

    /**
     * Retrieve the latest schema of a topic.
     *
     * @param topic topic name, in fully qualified format
     * @return latest schema
     * @throws PulsarAdminException
     */
    SchemaInfo getSchemaInfo(String topic) throws PulsarAdminException;

    /**
     * Retrieve the schema of a topic at a given <tt>version</tt>.
     *
     * @param topic topic name, in fully qualified format
     * @param version schema version
     * @return the schema info at a given <tt>version</tt>
     * @throws PulsarAdminException
     */
    SchemaInfo getSchemaInfo(String topic, long version) throws PulsarAdminException;

    /**
     * Delete the schema associated with a given <tt>topic</tt>.
     *
     * @param topic topic name, in fully qualified format
     * @throws PulsarAdminException
     */
    void deleteSchema(String topic) throws PulsarAdminException;

    /**
     * Create a schema for a given <tt>topic</tt> with the provided schema info.
     *
     * @param topic topic name, in fully qualified fomrat
     * @param schemaInfo schema info
     * @throws PulsarAdminException
     */
    void createSchema(String topic, SchemaInfo schemaInfo) throws PulsarAdminException;

    /**
     * Create a schema for a given <tt>topic</tt>.
     *
     * @param topic topic name, in fully qualified format
     * @param schemaPayload schema payload
     * @throws PulsarAdminException
     */
    void createSchema(String topic, PostSchemaPayload schemaPayload) throws PulsarAdminException;

}
