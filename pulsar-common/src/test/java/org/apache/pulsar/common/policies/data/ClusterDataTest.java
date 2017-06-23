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
import static org.testng.Assert.assertFalse;

import org.apache.pulsar.common.policies.data.ClusterData;
import org.testng.annotations.Test;

@Test
public class ClusterDataTest {

    @Test
    void simple() {
        String s1 = "http://broker.messaging.c1.example.com:8080";
        String s2 = "http://broker.messaging.c2.example.com:8080";
        String s3 = "https://broker.messaging.c1.example.com:4443";
        String s4 = "https://broker.messaging.c2.example.com:4443";
        ClusterData c = new ClusterData(s1);
        c.setServiceUrl(null);
        c.setServiceUrlTls(null);

        assertEquals(new ClusterData(s1), new ClusterData(s1));
        assertEquals(new ClusterData(s1).getServiceUrl(), s1);

        assertFalse(new ClusterData(s1).equals(new ClusterData()));
        assertFalse(new ClusterData(s1).equals(new ClusterData(s2)));
        assertFalse(new ClusterData(s1).equals(s1));

        assertEquals(new ClusterData(s1).hashCode(), new ClusterData(s1).hashCode());

        assertFalse(new ClusterData(s1).hashCode() == new ClusterData(s2).hashCode());

        assertFalse(new ClusterData(s1).hashCode() == c.hashCode());

        assertEquals(new ClusterData(s1, s3), new ClusterData(s1, s3));
        assertEquals(new ClusterData(s1, s3).getServiceUrl(), s1);
        assertEquals(new ClusterData(s1, s3).getServiceUrlTls(), s3);

        assertFalse(new ClusterData(s1, s3).equals(new ClusterData()));
        assertFalse(new ClusterData(s1, s3).equals(new ClusterData(s2, s4)));

        assertEquals(new ClusterData(s1, s3).hashCode(), new ClusterData(s1, s3).hashCode());
        assertFalse(new ClusterData(s1, s3).hashCode() == new ClusterData(s2, s4).hashCode());
        assertFalse(new ClusterData(s1, s3).hashCode() == new ClusterData(s1, s4).hashCode());

    }
}
