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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class V5UtilsTest {

    @Test
    public void testAsScalableTopicNameKeepsExistingTopicDomain() {
        TopicName tn = V5Utils.asScalableTopicName("topic://tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testAsScalableTopicNameReturnsSameInstanceWhenAlreadyTopicDomain() {
        // asScalableTopicName returns the parsed TopicName as-is when it already has the
        // topic:// domain. TopicName.get caches by fully-qualified name, so the same string
        // should yield an identical cached instance on the second call too.
        TopicName first = V5Utils.asScalableTopicName("topic://tenant/ns/my-topic");
        TopicName second = V5Utils.asScalableTopicName("topic://tenant/ns/my-topic");
        assertSame(first, second);
    }

    @Test
    public void testAsScalableTopicNameRewrapsBareLocalName() {
        // A bare name parses as persistent:// by default; V5 must rewrap it as topic://.
        TopicName tn = V5Utils.asScalableTopicName("my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getLocalName(), "my-topic");
        // default namespace is public/default
        assertEquals(tn.getTenant(), "public");
        assertEquals(tn.getNamespacePortion(), "default");
    }

    @Test
    public void testAsScalableTopicNameRewrapsFullyQualifiedNonTopicDomain() {
        // A persistent://... name must be rewrapped as topic://...
        TopicName tn = V5Utils.asScalableTopicName("persistent://tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testAsScalableTopicNameRewrapsShortForm() {
        // tenant/ns/my-topic (no scheme) is parsed with the default persistent:// prefix.
        TopicName tn = V5Utils.asScalableTopicName("tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }
}
