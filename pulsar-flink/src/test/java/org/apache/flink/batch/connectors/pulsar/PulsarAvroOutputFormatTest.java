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
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


/**
 * Tests for Pulsar Avro Output Format
 */
public class PulsarAvroOutputFormatTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorWhenServiceUrlIsNull() {
        new PulsarAvroOutputFormat(null, "testTopic", new AuthenticationDisabled());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorWhenTopicNameIsNull() {
        new PulsarAvroOutputFormat("testServiceUrl", null, new AuthenticationDisabled());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorWhenTopicNameIsBlank() {
        new PulsarAvroOutputFormat("testServiceUrl", " ", new AuthenticationDisabled());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorWhenServiceUrlIsBlank() {
        new PulsarAvroOutputFormat(" ", "testTopic", new AuthenticationDisabled());
    }

    @Test
    public void testPulsarAvroOutputFormatConstructor() {
        PulsarAvroOutputFormat pulsarAvroOutputFormat =
                new PulsarAvroOutputFormat("testServiceUrl", "testTopic", new AuthenticationDisabled());
        assertNotNull(pulsarAvroOutputFormat);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorV2WhenServiceUrlIsNull() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(null);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");

        new PulsarAvroOutputFormat(clientConf, producerConf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorV2WhenTopicNameIsNull() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(null);

        new PulsarAvroOutputFormat(clientConf, producerConf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorV2WhenTopicNameIsBlank() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(StringUtils.EMPTY);

        new PulsarAvroOutputFormat(clientConf, producerConf);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testPulsarAvroOutputFormatConstructorV2WhenServiceUrlIsBlank() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(StringUtils.EMPTY);

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName(StringUtils.EMPTY);

        new PulsarAvroOutputFormat(clientConf, producerConf);
    }

    @Test
    public void testPulsarAvroOutputFormatConstructorV2() {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("testServiceUrl");

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setTopicName("testTopic");

        PulsarAvroOutputFormat pulsarAvroOutputFormat = new PulsarAvroOutputFormat(clientConf, producerConf);
        assertNotNull(pulsarAvroOutputFormat);
    }
}
