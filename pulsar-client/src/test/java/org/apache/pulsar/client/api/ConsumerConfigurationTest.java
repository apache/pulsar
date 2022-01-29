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
package org.apache.pulsar.client.api;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;

import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Unit test of {@link ConsumerConfiguration}.
 */
public class ConsumerConfigurationTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerConfigurationTest.class);

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testJsonIgnore() throws Exception {

        ConsumerConfigurationData<?> conf = new ConsumerConfigurationData<>();
        conf.setConsumerEventListener(new ConsumerEventListener() {

            @Override
            public void becameActive(Consumer<?> consumer, int partitionId) {
            }

            @Override
            public void becameInactive(Consumer<?> consumer, int partitionId) {
            }
        });

        conf.setMessageListener((MessageListener) (consumer, msg) -> {
        });

        conf.setCryptoKeyReader(mock(CryptoKeyReader.class));

        ObjectMapper m = new ObjectMapper();
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();

        String confAsString = w.writeValueAsString(conf);
        log.info("conf : {}", confAsString);

        assertFalse(confAsString.contains("messageListener"));
        assertFalse(confAsString.contains("consumerEventListener"));
        assertFalse(confAsString.contains("cryptoKeyReader"));
    }

}
