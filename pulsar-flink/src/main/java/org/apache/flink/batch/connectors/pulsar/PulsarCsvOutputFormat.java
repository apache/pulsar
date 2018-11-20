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
package org.apache.flink.batch.connectors.pulsar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.batch.connectors.pulsar.serialization.CsvSerializationSchema;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * Flink Batch Sink to write DataSets into a Pulsar topic as Csv.
 */
public class PulsarCsvOutputFormat<T> extends BasePulsarOutputFormat<T> {

    private static final long serialVersionUID = -4461671510903404196L;

    public PulsarCsvOutputFormat(String serviceUrl, String topicName, List<String> fieldNames) {
        super(serviceUrl, topicName);
        Preconditions.checkNotNull(fieldNames,  "fieldNames cannot be null.");
        Preconditions.checkArgument(!fieldNames.isEmpty(),  "fieldNames cannot be empty.");
        Preconditions.checkArgument(fieldNames.stream().allMatch(fieldName -> StringUtils.isNotBlank(fieldName)),  "fieldNames cannot be null/blank.");

        this.serializationSchema = new CsvSerializationSchema(fieldNames);
    }

}
