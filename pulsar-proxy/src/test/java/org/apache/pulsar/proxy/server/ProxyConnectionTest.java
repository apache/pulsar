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
package org.apache.pulsar.proxy.server;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class ProxyConnectionTest {

    @Test
    public void testMatchesHostAndPort() {
        assertTrue(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:6650", "1.2.3.4:6650"));
        assertTrue(ProxyConnection
                .matchesHostAndPort("pulsar+ssl://", "pulsar+ssl://1.2.3.4:6650", "1.2.3.4:6650"));
        assertFalse(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:12345", "5.6.7.8:1234"));
        assertFalse(ProxyConnection
                .matchesHostAndPort("pulsar://", "pulsar://1.2.3.4:12345", "1.2.3.4:1234"));
    }
}
