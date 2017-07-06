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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import org.apache.pulsar.common.naming.DestinationDomain;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.annotations.Test;

@Test
public class NamespaceNameTest {

    @Test
    void namespace() {
        try {
            new NamespaceName("namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new NamespaceName("property.namespace");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new NamespaceName("0.0.0.0");
            fail("Should have caused exception");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            new NamespaceName("property.namespace:destination");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("property/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("property/cluster/namespace/destination");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName(null);
        } catch (IllegalArgumentException e) {
            // OK
        }

        try {
            new NamespaceName(null, "use", "ns1");
        } catch (IllegalArgumentException e) {
            // OK
        }

        assertEquals(new NamespaceName("prop/cluster/ns").getPersistentTopicName("ds"),
                "persistent://prop/cluster/ns/ds");

        try {
            new NamespaceName("prop/cluster/ns").getDestinationName(null, "ds");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        assertEquals(new NamespaceName("prop/cluster/ns").getDestinationName(DestinationDomain.persistent, "ds"),
                "persistent://prop/cluster/ns/ds");
        assertEquals(new NamespaceName("prop/cluster/ns"), new NamespaceName("prop/cluster/ns"));
        assertEquals(new NamespaceName("prop/cluster/ns").toString(), "prop/cluster/ns");
        assertFalse(new NamespaceName("prop/cluster/ns").equals("prop/cluster/ns"));

        assertEquals(new NamespaceName("prop", "cluster", "ns"), new NamespaceName("prop/cluster/ns"));
        assertEquals(new NamespaceName("prop/cluster/ns").getProperty(), "prop");
        assertEquals(new NamespaceName("prop/cluster/ns").getCluster(), "cluster");
        assertEquals(new NamespaceName("prop/cluster/ns").getLocalName(), "ns");

        try {
            new NamespaceName("ns").getProperty();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("ns").getCluster();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("ns").getLocalName();
            fail("old style namespace");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("_pulsar/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName(null, "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("", "cluster", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("/cluster/namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar//namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar", null, "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar", "", "namespace");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar/cluster/");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar", "cluster", null);
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        try {
            new NamespaceName("pulsar", "cluster", "");
            fail("Should have raised exception");
        } catch (IllegalArgumentException e) {
            // Ok
        }

        NamespaceName v2Namespace = new NamespaceName("pulsar/colo1/testns-1");
        assertEquals(v2Namespace.getProperty(), "pulsar");
        assertEquals(v2Namespace.getCluster(), "colo1");
        assertEquals(v2Namespace.getLocalName(), "testns-1");
    }
}
