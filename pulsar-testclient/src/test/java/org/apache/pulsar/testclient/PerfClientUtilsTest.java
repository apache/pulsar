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
package org.apache.pulsar.testclient;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PerfClientUtilsTest {

    public static class MyAuth implements Authentication {
        @Override
        public String getAuthMethodName() {
            return null;
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Test
    public void testClientCreation() throws Exception {

        final PerformanceBaseArguments args = new PerformanceBaseArguments() {
            @Override
            public void fillArgumentsFromProperties(Properties prop) {
            }
        };

        args.tlsHostnameVerificationEnable = true;
        args.authPluginClassName = MyAuth.class.getName();
        args.authParams = "params";
        args.enableBusyWait = true;
        args.maxConnections = 100;
        args.ioThreads = 16;
        args.listenerName = "listener";
        args.listenerThreads = 12;
        args.statsIntervalSeconds = Long.MAX_VALUE;
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.tlsTrustCertsFilePath = "path";
        args.tlsAllowInsecureConnection = true;

        final ClientBuilderImpl builder = (ClientBuilderImpl)PerfClientUtils.createClientBuilderFromArguments(args);
        final ClientConfigurationData conf = builder.getClientConfigurationData();

        Assert.assertTrue(conf.isTlsHostnameVerificationEnable());
        Assert.assertEquals(conf.getAuthPluginClassName(), MyAuth.class.getName());
        Assert.assertEquals(conf.getAuthParams(), "params");
        Assert.assertTrue(conf.isEnableBusyWait());
        Assert.assertEquals(conf.getConnectionsPerBroker(), 100);
        Assert.assertEquals(conf.getNumIoThreads(), 16);
        Assert.assertEquals(conf.getListenerName(), "listener");
        Assert.assertEquals(conf.getNumListenerThreads(), 12);
        Assert.assertEquals(conf.getStatsIntervalSeconds(), Long.MAX_VALUE);
        Assert.assertEquals(conf.getServiceUrl(), "pulsar+ssl://my-pulsar:6651");
        Assert.assertEquals(conf.getTlsTrustCertsFilePath(), "path");
        Assert.assertTrue(conf.isTlsAllowInsecureConnection());

    }
}