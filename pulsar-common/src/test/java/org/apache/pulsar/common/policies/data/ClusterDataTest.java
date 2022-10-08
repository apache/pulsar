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
    public void illegalUrl() {
        String illegalS1 = "/broker.messaging.c1.example.com:8080";
        String illegalS2 = "broker.messaging.c2.example.com:8080";
        String illegalS3 = "fdsafasfasdf";
        String illegalS4 = "pulsar://broker.messaging.c2.example.com:8080";
        String illegalS5 = "pulsar+ssl://broker.messaging.c2.example.com:8080";
        String illegalS6 = "http://broker.messaging.c2.example.com:8080";
        String illegalS7 = "https://broker.messaging.c2.example.com:8080";

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS1));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().proxyServiceUrl(illegalS1));

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS2));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().proxyServiceUrl(illegalS2));

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS3));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS3));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS3));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS3));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().proxyServiceUrl(illegalS3));

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS4));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS4));
        ClusterData.builder().brokerServiceUrl(illegalS4);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS4));
        ClusterData.builder().proxyServiceUrl(illegalS4);

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS5));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS5));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS5));
        ClusterData.builder().brokerServiceUrlTls(illegalS5);
        ClusterData.builder().proxyServiceUrl(illegalS5);

        ClusterData.builder().serviceUrl(illegalS6);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrlTls(illegalS6));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS6));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS6));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().proxyServiceUrl(illegalS6));

        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().serviceUrl(illegalS7));
        ClusterData.builder().serviceUrlTls(illegalS7);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrl(illegalS7));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().brokerServiceUrlTls(illegalS7));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClusterData.builder().proxyServiceUrl(illegalS7));
    }
}
