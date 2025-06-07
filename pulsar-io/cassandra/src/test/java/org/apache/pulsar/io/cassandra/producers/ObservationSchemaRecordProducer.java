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
package org.apache.pulsar.io.cassandra.producers;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ObservationSchemaRecordProducer extends AbstractGenericRecordProducer {

    public ObservationSchemaRecordProducer(String brokerUrl, String inputTopic) {
        super(brokerUrl, inputTopic);
    }

    @Override
    GenericRecord getValue() {
        String val = "Some random string";

        GenericRecord record = Schema.generic(getGenericSchemaInfo()).newRecordBuilder()
                .set("key", getKey())
                .set("observed", val)
                .build();

        return record;
    }

    @Override
    SchemaInfo getGenericSchemaInfo() {
        RecordSchemaBuilder recordSchemaBuilder =
                SchemaBuilder.record("airquality.observation");

        recordSchemaBuilder.field("key")
                .type(SchemaType.STRING).required();

        recordSchemaBuilder.field("observed")
                .type(SchemaType.STRING).optional();

        return recordSchemaBuilder.build(SchemaType.AVRO);
    }
}
