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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.policies.data.ClusterPolicies.ClusterUrl;
import org.testng.annotations.Test;

public class ClusterUrlTest {

    @Test
    public void testNoUrlIsEmpty() {
        // The broker rejects "migrated=true" when the target ClusterUrl is empty. A ClusterUrl with
        // no URL at all is the canonical empty value and must be reported as such.
        assertTrue(new ClusterUrl().isEmpty());
        assertTrue(new ClusterUrl(null, null, null, null).isEmpty());
    }

    @Test
    public void testBlankUrlsAreEmpty() {
        assertTrue(new ClusterUrl("", "", "", "").isEmpty());
        assertTrue(new ClusterUrl(" ", null, "", null).isEmpty());
    }

    @Test
    public void testHttpOnlyUrlIsNotEmpty() {
        // Only the HTTP endpoints configured: still a URL, so not empty.
        assertFalse(new ClusterUrl("http://green:8080", "https://green:8443", null, null).isEmpty());
        assertFalse(new ClusterUrl("http://green:8080", null, null, null).isEmpty());
    }

    @Test
    public void testBrokerOnlyUrlIsNotEmpty() {
        assertFalse(new ClusterUrl(null, null, "pulsar://green:6650", null).isEmpty());
        assertFalse(new ClusterUrl(null, null, null, "pulsar+ssl://green:6651").isEmpty());
    }

    @Test
    public void testFullUrlIsNotEmpty() {
        assertFalse(new ClusterUrl("http://green:8080", "https://green:8443",
                "pulsar://green:6650", "pulsar+ssl://green:6651").isEmpty());
    }
}
