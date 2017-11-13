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
package org.apache.pulsar.common.configuration;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.broker.ServiceConfiguration;

import org.testng.annotations.Test;

import java.util.Properties;

public class ServiceConfigurationTest {
    public class MockConfiguration implements PulsarConfiguration {
        private Properties properties = new Properties();

        private String zookeeperServers = "localhost:2181";
        private String globalZookeeperServers = "localhost:2184";
        private int brokerServicePort = 7650;
        private int brokerServicePortTls = 7651;
        private int webServicePort = 9080;
        private int webServicePortTls = 9443;

        @Override
        public Properties getProperties() {
            return properties;
        }

        @Override
        public void setProperties(Properties properties) {
            this.properties = properties;
        }
    }

    @Test
    public void testConfigurationConverting() throws Exception {
        MockConfiguration mockConfiguration = new MockConfiguration();
        ServiceConfiguration serviceConfiguration = ServiceConfiguration.convertFrom(mockConfiguration);

        // check whether converting correctly
        assertEquals(serviceConfiguration.getZookeeperServers(), "localhost:2181");
        assertEquals(serviceConfiguration.getGlobalZookeeperServers(), "localhost:2184");
        assertEquals(serviceConfiguration.getBrokerServicePort(), 7650);
        assertEquals(serviceConfiguration.getBrokerServicePortTls(), 7651);
        assertEquals(serviceConfiguration.getWebServicePort(), 9080);
        assertEquals(serviceConfiguration.getWebServicePortTls(), 9443);
    }

}
