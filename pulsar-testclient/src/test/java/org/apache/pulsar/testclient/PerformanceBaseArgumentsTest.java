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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pulsar.client.api.ProxyProtocol.SNI;


public class PerformanceBaseArgumentsTest {

    @Test
    public void testReadFromConfigFile() {

        AtomicBoolean called = new AtomicBoolean();

        final PerformanceBaseArguments args = new PerformanceBaseArguments() {
            @Override
            public void fillArgumentsFromProperties(Properties prop) {
                called.set(true);
            }
        };
        args.confFile = "./src/test/resources/perf_client1.conf";
        args.fillArgumentsFromProperties();
        Assert.assertTrue(called.get());
        Assert.assertEquals(args.serviceURL, "https://my-pulsar:8443/");
        Assert.assertEquals(args.authPluginClassName,
                "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth");
        Assert.assertEquals(args.authParams, "myparams");
        Assert.assertEquals(args.tlsTrustCertsFilePath, "./path");
        Assert.assertTrue(args.tlsAllowInsecureConnection);
        Assert.assertTrue(args.tlsHostnameVerificationEnable);
        Assert.assertEquals(args.proxyServiceURL, "https://my-proxy-pulsar:4443/");
        Assert.assertEquals(args.proxyProtocol, SNI);
    }

    @Test
    public void testReadFromConfigFileWithoutProxyUrl() {

        AtomicBoolean called = new AtomicBoolean();

        final PerformanceBaseArguments args = new PerformanceBaseArguments() {
            @Override
            public void fillArgumentsFromProperties(Properties prop) {
                called.set(true);
            }
        };
        args.confFile = "./src/test/resources/perf_client2.conf";
        args.fillArgumentsFromProperties();
        Assert.assertTrue(called.get());
        Assert.assertEquals(args.serviceURL, "https://my-pulsar:8443/");
        Assert.assertEquals(args.authPluginClassName,
                "org.apache.pulsar.testclient.PerfClientUtilsTest.MyAuth");
        Assert.assertEquals(args.authParams, "myparams");
        Assert.assertEquals(args.tlsTrustCertsFilePath, "./path");
        Assert.assertTrue(args.tlsAllowInsecureConnection);
        Assert.assertTrue(args.tlsHostnameVerificationEnable);
    }

    @Test
    public void testReadFromConfigFileProxyProtocolException() {

        AtomicBoolean calledVar1 = new AtomicBoolean();
        AtomicBoolean calledVar2 = new AtomicBoolean();

        final PerformanceBaseArguments args = new PerformanceBaseArguments() {
            @Override
            public void fillArgumentsFromProperties(Properties prop) {
                calledVar1.set(true);
            }
        };

        PerfClientUtils.setExitProcedure(code -> {
            calledVar2.set(true);
            Assert.assertNotNull(code);
            if (code != -1) {
                Assert.fail("Incorrect exit code");
            }
        });
        
        args.confFile = "./src/test/resources/perf_client3.conf";
        args.fillArgumentsFromProperties();
        Assert.assertTrue(calledVar1.get());
        Assert.assertTrue(calledVar2.get());
    }

}