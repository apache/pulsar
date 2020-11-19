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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Schema Provider.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface SchemaInfoProvider {

    /**
     * Retrieve the schema info of a given <tt>schemaVersion</tt>.
     *
     * @param schemaVersion schema version
     * @return schema info of the provided <tt>schemaVersion</tt>
     */
    CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion);

    /**
     * Retrieve the latest schema info.
     *
     * @return the latest schema
     */
    CompletableFuture<SchemaInfo> getLatestSchema();

    /**
     * Retrieve the topic name.
     *
     * @return the topic name
     */
    String getTopicName();

}
