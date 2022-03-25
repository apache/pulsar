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
package org.apache.pulsar.tests.integration.schema;

import com.google.common.collect.Sets;
import java.time.temporal.ChronoUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.testng.Assert.assertEquals;

@Slf4j
public class JodaTimeTest extends PulsarTestSuite {

    private PulsarClient client;
    private PulsarAdmin admin;

    public void setupCluster() throws Exception {
        super.setupCluster();
        this.client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();
        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();
    }

    @Override
    public void tearDownCluster() throws Exception {
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
        super.tearDownCluster();
    }

    @Data
    private static class JodaSchema {

        @org.apache.avro.reflect.AvroSchema("{\n" +
                "  \"type\": \"bytes\",\n" +
                "  \"logicalType\": \"decimal\",\n" +
                "  \"precision\": 4,\n" +
                "  \"scale\": 2\n" +
                "}")
        BigDecimal decimal;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"date\"}")
        LocalDate date;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        DateTime timestampMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"time-millis\"}")
        LocalTime timeMillis;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        long timestampMicros;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"long\",\"logicalType\":\"time-micros\"}")
        long timeMicros;
    }

    @Test
    public void testJodaTime() throws PulsarAdminException, PulsarClientException {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topic = "test-joda-time-schema";
        final String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(pulsarCluster.getClusterName())
        );

        JodaSchema forSend = new JodaSchema();
        forSend.setDecimal(new BigDecimal("12.34"));
        forSend.setTimeMicros(System.currentTimeMillis() * 1000);
        forSend.setTimestampMillis(new DateTime("2019-03-26T04:39:58.469Z", ISOChronology.getInstanceUTC()));
        forSend.setTimeMillis(LocalTime.now().truncatedTo(ChronoUnit.MILLIS));
        forSend.setTimeMicros(System.currentTimeMillis() * 1000);
        forSend.setDate(LocalDate.now());

        Producer<JodaSchema> producer = client
                .newProducer(Schema.AVRO(JodaSchema.class))
                .topic(fqtn)
                .create();

        Consumer<JodaSchema> consumer = client
                .newConsumer(Schema.AVRO(JodaSchema.class))
                .topic(fqtn)
                .subscriptionName("test")
                .subscribe();

        producer.send(forSend);
        JodaSchema received = consumer.receive().getValue();
        assertEquals(received, forSend);

        producer.close();
        consumer.close();

        log.info("Successfully Joda time logical type message : {}", received);
    }
}
