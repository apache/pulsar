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
package org.apache.pulsar.common.naming;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

public class NamespaceNameTest {

    @Test
    public void namespace() {
        try {
            NamespaceName.get("namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("property.namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("0.0.0.0");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NamespaceName.get("property.namespace:topic");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("property/cluster/namespace/topic");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get(null);
        } catch (IllegalArgumentException e) {
            // OK
        }

        try {
            NamespaceName.get(null, "use", "ns1");
        } catch (IllegalArgumentException e) {
            // OK
        }

        assertEquals(NamespaceName.get("prop/cluster/ns").getPersistentTopicName("ds"),
                "persistent://prop/cluster/ns/ds");

        try {
            NamespaceName.get("prop/cluster/ns").getTopicName(null, "ds");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        assertEquals(NamespaceName.get("prop/cluster/ns").getTopicName(TopicDomain.persistent, "ds"),
                "persistent://prop/cluster/ns/ds");
        assertEquals(NamespaceName.get("prop/cluster/ns"), NamespaceName.get("prop/cluster/ns"));
        assertEquals(NamespaceName.get("prop/cluster/ns").toString(), "prop/cluster/ns");
        assertNotEquals(NamespaceName.get("prop/cluster/ns"), "prop/cluster/ns");

        assertEquals(NamespaceName.get("prop", "cluster", "ns"), NamespaceName.get("prop/cluster/ns"));
        assertEquals(NamespaceName.get("prop/cluster/ns").getTenant(), "prop");
        assertEquals(NamespaceName.get("prop/cluster/ns").getCluster(), "cluster");
        assertEquals(NamespaceName.get("prop/cluster/ns").getLocalName(), "ns");

        try {
            NamespaceName.get("ns").getTenant();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("ns").getCluster();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("ns").getLocalName();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get(null, "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("", "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar//namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", null, "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "cluster", null);
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            NamespaceName.get("pulsar", "cluster", "");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        NamespaceName v2Namespace = NamespaceName.get("pulsar/colo1/testns-1");
        assertEquals(v2Namespace.getTenant(), "pulsar");
        assertEquals(v2Namespace.getCluster(), "colo1");
        assertEquals(v2Namespace.getLocalName(), "testns-1");
    }

    @Test
    void testNewScheme() {
        NamespaceName ns = NamespaceName.get("my-tenant/my-namespace");
        assertEquals(ns.getTenant(), "my-tenant");
        assertEquals(ns.getLocalName(), "my-namespace");
        assertTrue(ns.isGlobal());
        assertNull(ns.getCluster());
        assertEquals(ns.getPersistentTopicName("my-topic"), "persistent://my-tenant/my-namespace/my-topic");
    }
}
