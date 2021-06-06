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
}
