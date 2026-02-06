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

import java.util.Random;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ReadingSchemaRecordProducer extends AbstractGenericRecordProducer {

    private Random rnd = new Random();

    int lastReadingId = rnd.nextInt(50000);

    public ReadingSchemaRecordProducer(String brokerUrl, String inputTopic) {
        super(brokerUrl, inputTopic);
    }

    @Override
    GenericRecord getValue() {
        GenericRecord record = Schema.generic(getGenericSchemaInfo())
                .newRecordBuilder()
                .set("readingid", lastReadingId++ + "")
                .set("avg_ozone", rnd.nextDouble())
                .set("min_ozone", rnd.nextDouble())
                .set("max_ozone", rnd.nextDouble())
                .set("avg_pm10", rnd.nextDouble())
                .set("min_pm10", rnd.nextDouble())
                .set("max_pm10", rnd.nextDouble())
                .set("avg_pm25", rnd.nextDouble())
                .set("min_pm25", rnd.nextDouble())
                .set("max_pm25", rnd.nextDouble())
                .set("local_time_zone", "PST")
                .set("state_code", "CA")
                .set("reporting_area", lastReadingId + "")
                .set("hour_observed", rnd.nextInt(24))
                .set("date_observed", "2022-06-18")
                .set("latitude", Float.valueOf(40.021f))
                .set("longitude", Float.valueOf(-122.33f))
                .build();

        return record;
    }

    @Override
    SchemaInfo getGenericSchemaInfo() {
        RecordSchemaBuilder schemaBuilder =
                SchemaBuilder.record("airquality.reading");

        schemaBuilder.field("readingid").type(SchemaType.STRING).required();
        schemaBuilder.field("avg_ozone").type(SchemaType.DOUBLE);
        schemaBuilder.field("min_ozone").type(SchemaType.DOUBLE);
        schemaBuilder.field("max_ozone").type(SchemaType.DOUBLE);
        schemaBuilder.field("avg_pm10").type(SchemaType.DOUBLE);
        schemaBuilder.field("min_pm10").type(SchemaType.DOUBLE);
        schemaBuilder.field("max_pm10").type(SchemaType.DOUBLE);
        schemaBuilder.field("avg_pm25").type(SchemaType.DOUBLE);
        schemaBuilder.field("min_pm25").type(SchemaType.DOUBLE);
        schemaBuilder.field("max_pm25").type(SchemaType.DOUBLE);

        schemaBuilder.field("local_time_zone").type(SchemaType.STRING);
        schemaBuilder.field("state_code").type(SchemaType.STRING);
        schemaBuilder.field("reporting_area").type(SchemaType.STRING).required();
        schemaBuilder.field("hour_observed").type(SchemaType.INT32);
        schemaBuilder.field("date_observed").type(SchemaType.STRING);
        schemaBuilder.field("latitude").type(SchemaType.FLOAT);
        schemaBuilder.field("longitude").type(SchemaType.FLOAT);

        return schemaBuilder.build(SchemaType.AVRO);
    }
}
