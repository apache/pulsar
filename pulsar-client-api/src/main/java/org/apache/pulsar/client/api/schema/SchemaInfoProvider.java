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

import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Schema Provider.
 */
public interface SchemaInfoProvider {

    /**
     * Retrieve the schema info of a given <tt>schemaVersion</tt>.
     *
     * @param schemaVersion schema version
     * @return schema info of the provided <tt>schemaVersion</tt>
     */
    SchemaInfo getSchemaByVersion(byte[] schemaVersion);

    /**
     * Retrieve the latest schema info.
     *
     * @return the latest schema
     */
    SchemaInfo getLatestSchema();

    /**
     * Retrieve the topic name.
     *
     * @return the topic name
     */
    public String getTopicName();

}
