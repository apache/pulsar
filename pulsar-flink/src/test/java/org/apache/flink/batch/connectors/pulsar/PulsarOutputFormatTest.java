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

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for PulsarOutputFormat
 */
public class PulsarOutputFormatTest {

    @Test(expected = NullPointerException.class)
    public void testPulsarOutputFormatConstructorWhenServiceUrlIsNull() {
        new PulsarOutputFormat(null, "testTopic", text -> text.toString().getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenTopicNameIsNull() {
        new PulsarOutputFormat("testServiceUrl", null, text -> text.toString().getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPulsarOutputFormatConstructorWhenTopicNameIsBlank() {
        new PulsarOutputFormat("testServiceUrl", " ", text -> text.toString().getBytes());
    }

    @Test(expected = NullPointerException.class)
    public void testPulsarOutputFormatConstructorWhenSerializationSchemaIsNull() {
        new PulsarOutputFormat("testServiceUrl", "testTopic", null);
    }

    @Test
    public void testPulsarOutputFormatConstructor() {
        PulsarOutputFormat pulsarOutputFormat =
                new PulsarOutputFormat("testServiceUrl", "testTopic", text -> text.toString().getBytes());
        assertNotNull(pulsarOutputFormat);
    }

}
