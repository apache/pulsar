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
package org.apache.pulsar.common.topics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.regex.Pattern;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class TopicsPatternTest {

    @Test
    public void testNamespace() {
        TopicsPattern pattern = TopicsPatternFactory.create(Pattern.compile("tenant/namespace/topic-.*"));
        NamespaceName namespace = pattern.namespace();
        assertEquals(namespace.getTenant(), "tenant");
        assertEquals(namespace.getLocalName(), "namespace");
        assertEquals(namespace.toString(), "tenant/namespace");

        // Test with a standard topic pattern
        TopicsPattern domainPrefixPattern =
                TopicsPatternFactory.create(Pattern.compile("persistent://tenant/namespace/topic-.*"));
        NamespaceName namespace2 = pattern.namespace();
        assertEquals(namespace.getTenant(), "tenant");
        assertEquals(namespace.getLocalName(), "namespace");
        assertEquals(namespace.toString(), "tenant/namespace");

        // Test with a more complex topic pattern
        TopicsPattern complexPattern =
                TopicsPatternFactory.create(Pattern.compile("persistent://my-tenant/my-namespace/prefix-.*-suffix"));
        NamespaceName complexNamespace = complexPattern.namespace();
        assertEquals(complexNamespace.getTenant(), "my-tenant");
        assertEquals(complexNamespace.getLocalName(), "my-namespace");
        assertEquals(complexNamespace.toString(), "my-tenant/my-namespace");

        // Test with non-persistent topic pattern
        TopicsPattern nonPersistentPattern =
                TopicsPatternFactory.create(Pattern.compile("non-persistent://test-tenant/test-namespace/.*"));
        NamespaceName nonPersistentNamespace = nonPersistentPattern.namespace();
        assertEquals(nonPersistentNamespace.getTenant(), "test-tenant");
        assertEquals(nonPersistentNamespace.getLocalName(), "test-namespace");
        assertEquals(nonPersistentNamespace.toString(), "test-tenant/test-namespace");
    }

    @Test
    public void testTopicLookupNameForTopicListWatcherPlacement() {
        TopicsPattern pattern =
                TopicsPatternFactory.create(Pattern.compile("persistent://tenant/namespace/topic-.*(\\d+)?"));
        String lookupName = pattern.topicLookupNameForTopicListWatcherPlacement();
        assertNotNull(lookupName);
        TopicName lookupTopicName = TopicName.get(lookupName);
        assertEquals(lookupTopicName.getTenant(), "tenant");
        assertEquals(lookupTopicName.getNamespacePortion(), "namespace");
        assertEquals(lookupTopicName.getLocalName(), "topic-.*(\\d+)?");
    }
}