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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;

/**
 * Auto detect schema, returns only GenericRecord instances.
 */
@Slf4j
public class AutoConsumeSchema extends AbstractAutoConsumeSchema<GenericRecord> {

    @Override
    public Schema<GenericRecord> clone() {
        Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
        if (this.schema != null) {
            schema.configureSchemaInfo(topicName, componentName, this.schema.getSchemaInfo());
        } else {
            schema.configureSchemaInfo(topicName, componentName, null);
        }
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return true;
    }

}
