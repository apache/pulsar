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
package org.apache.pulsar.common.naming;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class NamespaceNameTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_invalidFormat() {
        NamespaceName.get("namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_propertyNamespace() {
        NamespaceName.get("property.namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_loopBackAddress() {
        NamespaceName.get("0.0.0.0");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_propertyNamespaceTopic() {
        NamespaceName.get("property.namespace:topic");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_propertyClusterNamespaceTopic() {
        NamespaceName.get("property/cluster/namespace/topic");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_null() {
        NamespaceName.get(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_nullTenant() {
        NamespaceName.get(null, "use", "ns1");
    }

    @Test
    public void namespace_persistentTopic() {
        assertEquals(NamespaceName.get("prop/cluster/ns").getPersistentTopicName("ds"),
                "persistent://prop/cluster/ns/ds");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_topicNameNullDomain() {
        NamespaceName.get("prop/cluster/ns").getTopicName(null, "ds");
    }

    @Test
    public void namespace_persistentTopicExplicitDomain() {
        assertEquals(NamespaceName.get("prop/cluster/ns").getTopicName(TopicDomain.persistent, "ds"),
                "persistent://prop/cluster/ns/ds");
    }

    @Test
    public void namespace_equals() {
        assertEquals(NamespaceName.get("prop/cluster/ns"), NamespaceName.get("prop/cluster/ns"));
    }

    @Test
    public void namespace_toString() {
        assertEquals(NamespaceName.get("prop/cluster/ns").toString(), "prop/cluster/ns");
    }

    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    public void namespace_equalsCheckType() {
        assertNotEquals(NamespaceName.get("prop/cluster/ns"), "prop/cluster/ns");
    }

    @Test
    public void namespace_vargEquivalentToParse() {
        assertEquals(NamespaceName.get("prop", "cluster", "ns"), NamespaceName.get("prop/cluster/ns"));
    }

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
    @Test
    public void namespace_members() {
        assertEquals(NamespaceName.get("prop/cluster/ns").getTenant(), "prop");
        assertEquals(NamespaceName.get("prop/cluster/ns").getCluster(), "cluster");
        assertEquals(NamespaceName.get("prop/cluster/ns").getLocalName(), "ns");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_oldStyleNamespaceTenant() {
        NamespaceName.get("ns").getTenant();
    }

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_oldStyleNamespaceCluster() {
        NamespaceName.get("ns").getCluster();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_oldStyleNamespaceLocalName() {
        NamespaceName.get("ns").getLocalName();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_nullTenant2() {
        NamespaceName.get(null, "cluster", "namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_emptyTenant() {
        NamespaceName.get("", "cluster", "namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_emptyTenantElement() {
        NamespaceName.get("/cluster/namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_missingCluster() {
        NamespaceName.get("pulsar//namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_nullCluster() {
        NamespaceName.get("pulsar", null, "namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_emptyCluster() {
        NamespaceName.get("pulsar", "", "namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_nullNamespace() {
        NamespaceName.get("pulsar", "cluster", null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_emptyNamespace() {
        NamespaceName.get("pulsar", "cluster", "");
    }

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
    @Test
    public void namespace_v2Namespace() {
        NamespaceName v2Namespace = NamespaceName.get("pulsar/colo1/testns-1");
        assertEquals(v2Namespace.getTenant(), "pulsar");
        assertEquals(v2Namespace.getCluster(), "colo1");
        assertEquals(v2Namespace.getLocalName(), "testns-1");
    }

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
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
