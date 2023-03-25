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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class ClusterDataTest {

    String httpProtocolUrl = "http://broker.messaging.c2.example.com:8080";
    String httpsProtocolUrl = "https://broker.messaging.c2.example.com:8080";
    String pulsarProtocolUrl = "pulsar://broker.messaging.c2.example.com:8080";
    String pulsarOverSslProtocolUrl = "pulsar+ssl://broker.messaging.c2.example.com:8080";

    @Test
    public void simple() {
        String s1 = "http://broker.messaging.c1.example.com:8080";
        String s2 = "http://broker.messaging.c2.example.com:8080";
        String s3 = "https://broker.messaging.c1.example.com:4443";
        String s4 = "https://broker.messaging.c2.example.com:4443";
        ClusterData c = ClusterData.builder()
                .serviceUrl(null)
                .serviceUrlTls(null)
                .build();

        assertEquals(ClusterData.builder().serviceUrl(s1).build(), ClusterData.builder().serviceUrl(s1).build());
        assertEquals(ClusterData.builder().serviceUrl(s1).build().getServiceUrl(), s1);

        assertNotEquals(ClusterData.builder().build(),
                ClusterData.builder().serviceUrl(s1).build());
        assertNotEquals(ClusterData.builder().serviceUrl(s2).build(),
                ClusterData.builder().serviceUrl(s1).build());
        assertNotEquals(s1, ClusterData.builder().serviceUrl(s1).build());

        assertEquals(ClusterData.builder().serviceUrl(s1).build().hashCode(),
                ClusterData.builder().serviceUrl(s1).build().hashCode());

        assertNotEquals(ClusterData.builder().serviceUrl(s2).build().hashCode(),
                ClusterData.builder().serviceUrl(s1).build().hashCode());

        assertNotEquals(c.hashCode(), ClusterData.builder().serviceUrl(s1).build().hashCode());

        assertEquals(ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build(),
                ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build());
        assertEquals(ClusterData.builder()
                .serviceUrl(s1)
                .serviceUrlTls(s3)
                .build().getServiceUrl(), s1);
        assertEquals(ClusterData.builder()
                .serviceUrl(s1)
                .serviceUrlTls(s3)
                .build().getServiceUrlTls(), s3);

        assertNotEquals(ClusterData.builder().build(), ClusterData.builder()
                .serviceUrl(s1)
                .serviceUrlTls(s3)
                .build());
        assertNotEquals(ClusterData.builder()
                        .serviceUrl(s2)
                        .serviceUrlTls(s4)
                        .build(),
                ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build());

        assertEquals(ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build().hashCode(),
                ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build().hashCode());
        assertNotEquals(ClusterData.builder()
                        .serviceUrl(s2)
                        .serviceUrlTls(s4)
                        .build().hashCode(),
                ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build().hashCode());
        assertNotEquals(ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s4)
                        .build().hashCode(),
                ClusterData.builder()
                        .serviceUrl(s1)
                        .serviceUrlTls(s3)
                        .build().hashCode());

    }

    @Test
    public void testCheckProperties() {
        String url1 = "/broker.messaging.c1.example.com:8080";
        String url2 = "broker.messaging.c2.example.com:8080";
        String url3 = "fdsafasfasdf";
        String url4 = "pulsar://broker.messaging.c2.example.com:8080";
        String url5 = "pulsar+ssl://broker.messaging.c2.example.com:8080";
        String url6 = "http://broker.messaging.c2.example.com:8080";
        String url7 = "https://broker.messaging.c2.example.com:8080";

        ClusterDataImpl.builder()
                .serviceUrl(httpProtocolUrl)
                .brokerServiceUrl(pulsarProtocolUrl)
                .proxyServiceUrl(pulsarProtocolUrl)
                .build()
                .checkPropertiesIfPresent();

        ClusterDataImpl.builder()
                .serviceUrlTls(httpsProtocolUrl)
                .brokerServiceUrlTls(pulsarOverSslProtocolUrl)
                .proxyServiceUrl(pulsarOverSslProtocolUrl)
                .build()
                .checkPropertiesIfPresent();

        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url1).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url1).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url1).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url1).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url1).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url2).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url2).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url2).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url2).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url2).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url3).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url3).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url3).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url3).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url3).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url4).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url4).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url4).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url5).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url5).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url5).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url6).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url6).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url6).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url6).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url7).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url7).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url7).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url7).build().checkPropertiesIfPresent());
    }

    @DataProvider(name = "test-service-url")
    public Object[][] dataProviderForServiceUrlTest() {
        /*
         *      1. testArgumentDetails      String
         *      2. testSubject              ClusterDataImpl
         *      3. isExceptionExpected      Boolean
         *      4. expectedExceptionClass   Class
         * */
        return new Object[][]{
                {
                        "valid ServiceUrl and ServiceUrlTls as null",
                        ClusterDataImpl.builder()
                                .serviceUrl(httpProtocolUrl)
                                .serviceUrlTls(null)
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        false,
                        null
                },
                {
                        "ServiceUrl as null and valid ServiceUrlTls",
                        ClusterDataImpl.builder()
                                .serviceUrl(null)
                                .serviceUrlTls(httpsProtocolUrl)
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        false,
                        null
                },
                {
                        "Both ServiceURL and ServiceUrlTls as null.",
                        ClusterDataImpl.builder()
                                .serviceUrl(null)
                                .serviceUrlTls(null)
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "Both ServiceURL and ServiceUrlTls as empty string.",
                        ClusterDataImpl.builder()
                                .serviceUrl("")
                                .serviceUrlTls("")
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "invalid ServiceURL and ServiceUrlTls as null.",
                        ClusterDataImpl.builder()
                                .serviceUrl("broker.messaging.c2.example.com:8080")
                                .serviceUrlTls(null)
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "ServiceURL as null and invalid ServiceUrlTls.",
                        ClusterDataImpl.builder()
                                .serviceUrl(null)
                                .serviceUrlTls("broker.messaging.c2.example.com:8080")
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                }
        };
    }

    @Test(dataProvider = "test-service-url")
    public void testServiceUrl(Object[] args) {
        String testDetail = (String) args[0];
        ClusterDataImpl clusterDataImpl = (ClusterDataImpl) args[1];
        Boolean isExceptionExpected = (Boolean) args[2];
        Class expectedException = (Class) args[3];

        log.debug("Test : testServiceUrl where " + testDetail);

        if (isExceptionExpected) {
            Assert.assertThrows(expectedException, () -> clusterDataImpl.checkPropertiesIfPresent());
        } else {
            clusterDataImpl.checkPropertiesIfPresent();
        }
    }

    @DataProvider(name = "test-broker-service-url")
    public Object[][] dataProviderForBrokerServiceUrlTest() {
        /*
         *      1. testArgumentDetails      String
         *      2. testSubject              ClusterDataImpl
         *      3. isExceptionExpected      Boolean
         *      4. expectedExceptionClass   Class
         * */
        return new Object[][]{
                {
                        "valid BrokerServiceUrl and BrokerServiceUrlTls as null",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl(pulsarProtocolUrl)
                                .brokerServiceUrlTls(null)
                                .serviceUrl(httpProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        false,
                        null
                },
                {
                        "BrokerServiceUrl as null and valid BrokerServiceUrlTls",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl(null)
                                .brokerServiceUrlTls(pulsarOverSslProtocolUrl)
                                .serviceUrlTls(httpsProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        false,
                        null
                },
                {
                        "Both BrokerServiceURL and BrokerServiceUrlTls as null.",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl(null)
                                .brokerServiceUrlTls(null)
                                .serviceUrlTls(pulsarProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "Both BrokerServiceURL and BrokerServiceUrlTls as empty string.",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl("")
                                .brokerServiceUrlTls("")
                                .serviceUrl(httpProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "invalid BrokerServiceURL and BrokerServiceUrlTls as null.",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl("broker.messaging.c2.example.com:8080")
                                .brokerServiceUrlTls(null)
                                .serviceUrlTls(httpsProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                },
                {
                        "BrokerServiceURL as null and invalid BrokerServiceUrlTls.",
                        ClusterDataImpl.builder()
                                .brokerServiceUrl(null)
                                .brokerServiceUrlTls("broker.messaging.c2.example.com:8080")
                                .serviceUrlTls(httpsProtocolUrl)
                                .proxyServiceUrl(pulsarProtocolUrl)
                                .build(),
                        true,
                        IllegalArgumentException.class
                }
        };
    }

    @Test(dataProvider = "test-broker-service-url")
    public void testBrokerServiceUrl(Object[] args) {
        String testDetail = (String) args[0];
        ClusterDataImpl clusterDataImpl = (ClusterDataImpl) args[1];
        Boolean isExceptionExpected = (Boolean) args[2];
        Class expectedException = (Class) args[3];

        log.debug("Test : testBrokerServiceUrl where " + testDetail);

        if (isExceptionExpected) {
            Assert.assertThrows(expectedException, () -> clusterDataImpl.checkPropertiesIfPresent());
        } else {
            clusterDataImpl.checkPropertiesIfPresent();
        }
    }
}
