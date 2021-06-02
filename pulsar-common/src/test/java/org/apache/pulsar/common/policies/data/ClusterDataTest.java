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
        ClusterDataImpl c = new ClusterDataImpl(s1);
        c.setServiceUrl(null);
        c.setServiceUrlTls(null);

        assertEquals(new ClusterDataImpl(s1), new ClusterDataImpl(s1));
        assertEquals(new ClusterDataImpl(s1).getServiceUrl(), s1);

        assertNotEquals(new ClusterDataImpl(), new ClusterDataImpl(s1));
        assertNotEquals(new ClusterDataImpl(s2), new ClusterDataImpl(s1));
        assertNotEquals(s1, new ClusterDataImpl(s1));

        assertEquals(new ClusterDataImpl(s1).hashCode(), new ClusterDataImpl(s1).hashCode());

        assertNotEquals(new ClusterDataImpl(s2).hashCode(), new ClusterDataImpl(s1).hashCode());

        assertNotEquals(c.hashCode(), new ClusterDataImpl(s1).hashCode());

        assertEquals(new ClusterDataImpl(s1, s3), new ClusterDataImpl(s1, s3));
        assertEquals(new ClusterDataImpl(s1, s3).getServiceUrl(), s1);
        assertEquals(new ClusterDataImpl(s1, s3).getServiceUrlTls(), s3);

        assertNotEquals(new ClusterDataImpl(), new ClusterDataImpl(s1, s3));
        assertNotEquals(new ClusterDataImpl(s2, s4), new ClusterDataImpl(s1, s3));

        assertEquals(new ClusterDataImpl(s1, s3).hashCode(), new ClusterDataImpl(s1, s3).hashCode());
        assertNotEquals(new ClusterDataImpl(s2, s4).hashCode(), new ClusterDataImpl(s1, s3).hashCode());
        assertNotEquals(new ClusterDataImpl(s1, s4).hashCode(), new ClusterDataImpl(s1, s3).hashCode());

    }
}
