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

    // 3-part V1 namespace names are no longer supported
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_threePartNameRejected() {
        NamespaceName.get("property/cluster/namespace");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_fourPartNameRejected() {
        NamespaceName.get("property/cluster/namespace/topic");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_null() {
        NamespaceName.get(null);
    }

    @Test
    public void namespace_persistentTopic() {
        assertEquals(NamespaceName.get("prop/ns").getPersistentTopicName("ds"),
                "persistent://prop/ns/ds");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_topicNameNullDomain() {
        NamespaceName.get("prop/ns").getTopicName(null, "ds");
    }

    @Test
    public void namespace_persistentTopicExplicitDomain() {
        assertEquals(NamespaceName.get("prop/ns").getTopicName(TopicDomain.persistent, "ds"),
                "persistent://prop/ns/ds");
    }

    @Test
    public void namespace_equals() {
        assertEquals(NamespaceName.get("prop/ns"), NamespaceName.get("prop/ns"));
    }

    @Test
    public void namespace_toString() {
        assertEquals(NamespaceName.get("prop/ns").toString(), "prop/ns");
    }

    @SuppressWarnings("AssertBetweenInconvertibleTypes")
    @Test
    public void namespace_equalsCheckType() {
        assertNotEquals(NamespaceName.get("prop/ns"), "prop/ns");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_oldStyleNamespaceTenant() {
        NamespaceName.get("ns").getTenant();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_oldStyleNamespaceLocalName() {
        NamespaceName.get("ns").getLocalName();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void namespace_emptyTenantElement() {
        NamespaceName.get("/namespace");
    }

    @Test
    void testNamespaceProperties() {
        NamespaceName ns = NamespaceName.get("my-tenant/my-namespace");
        assertEquals(ns.getTenant(), "my-tenant");
        assertEquals(ns.getLocalName(), "my-namespace");
        assertEquals(ns.getPersistentTopicName("my-topic"), "persistent://my-tenant/my-namespace/my-topic");
    }
}
