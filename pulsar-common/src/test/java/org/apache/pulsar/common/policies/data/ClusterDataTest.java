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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterDataTest {

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
        ClusterDataImpl.builder().brokerServiceUrl(url4).build().checkPropertiesIfPresent();
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url4).build().checkPropertiesIfPresent());
        ClusterDataImpl.builder().proxyServiceUrl(url4).build().checkPropertiesIfPresent();
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrl(url5).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().serviceUrlTls(url5).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url5).build().checkPropertiesIfPresent());
        ClusterDataImpl.builder().brokerServiceUrlTls(url5).build().checkPropertiesIfPresent();
        ClusterDataImpl.builder().proxyServiceUrl(url5).build().checkPropertiesIfPresent();
        ClusterDataImpl.builder().serviceUrl(url6).build().checkPropertiesIfPresent();
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
        ClusterDataImpl.builder().serviceUrlTls(url7).build().checkPropertiesIfPresent();
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrl(url7).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().brokerServiceUrlTls(url7).build().checkPropertiesIfPresent());
        Assert.assertThrows(IllegalArgumentException.class, () ->
                ClusterDataImpl.builder().proxyServiceUrl(url7).build().checkPropertiesIfPresent());
    }
}
