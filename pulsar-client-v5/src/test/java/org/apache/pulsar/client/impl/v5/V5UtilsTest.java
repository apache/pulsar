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
import static org.testng.Assert.expectThrows;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

public class V5UtilsTest {

    @Test
    public void testParseTopicInputAcceptsTopicDomain() {
        // topic://... is the canonical scalable form — passed through unchanged.
        TopicName tn = V5Utils.parseScalableTopicInput("topic://tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.topic);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testParseTopicInputAcceptsPersistentDomain() {
        // persistent://... is preserved — the broker decides whether the lookup
        // resolves to a real DAG (migrated topic) or a synthetic layout (regular topic).
        TopicName tn = V5Utils.parseScalableTopicInput("persistent://tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.persistent);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testParseTopicInputNormalisesBareNameToPersistentDefault() {
        // A bare local name normalises to persistent://public/default/... — same as v4.
        TopicName tn = V5Utils.parseScalableTopicInput("my-topic");
        assertEquals(tn.getDomain(), TopicDomain.persistent);
        assertEquals(tn.getTenant(), "public");
        assertEquals(tn.getNamespacePortion(), "default");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testParseTopicInputNormalisesShortFormToPersistent() {
        // tenant/ns/my-topic (no scheme) normalises to persistent://tenant/ns/my-topic.
        TopicName tn = V5Utils.parseScalableTopicInput("tenant/ns/my-topic");
        assertEquals(tn.getDomain(), TopicDomain.persistent);
        assertEquals(tn.getTenant(), "tenant");
        assertEquals(tn.getNamespacePortion(), "ns");
        assertEquals(tn.getLocalName(), "my-topic");
    }

    @Test
    public void testParseTopicInputRejectsNonPersistent() {
        // Scalable topics are always backed by managed ledgers — non-persistent must be
        // rejected at the SDK builder boundary, not deferred to a broker error.
        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () ->
                V5Utils.parseScalableTopicInput("non-persistent://tenant/ns/my-topic"));
        assertEquals(ex.getMessage(),
                "V5 does not support non-persistent:// topics: non-persistent://tenant/ns/my-topic");
    }

    @Test
    public void testToV5ExceptionKeepsTheErrorCategory() {
        var authorization = new org.apache.pulsar.client.api.PulsarClientException.AuthorizationException("authz");
        var authentication = new org.apache.pulsar.client.api.PulsarClientException.AuthenticationException("authn");
        var notFound = new org.apache.pulsar.client.api.PulsarClientException.NotFoundException("not found");

        assertConverted(V5Utils.toV5Exception(new CompletionException(authorization)),
                PulsarClientException.AuthorizationException.class, authorization);
        assertConverted(V5Utils.toV5Exception(new ExecutionException(authentication)),
                PulsarClientException.AuthenticationException.class, authentication);
        assertConverted(V5Utils.toV5Exception(notFound), PulsarClientException.NotFoundException.class, notFound);
    }

    @Test
    public void testToV5ExceptionWrapsOtherFailuresInThePlainException() {
        var busy = new org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException("busy");

        assertConverted(V5Utils.toV5Exception(new CompletionException(busy)), PulsarClientException.class, busy);
    }

    @Test
    public void testToV5ExceptionReturnsV5ExceptionsAsIs() {
        var closed = new PulsarClientException.AlreadyClosedException("closed");

        assertSame(V5Utils.toV5Exception(new CompletionException(closed)), closed);
    }

    private static void assertConverted(PulsarClientException converted, Class<?> expectedType,
                                        Throwable v4Exception) {
        assertEquals(converted.getClass(), expectedType);
        assertEquals(converted.getMessage(), v4Exception.getMessage());
        assertSame(converted.getCause(), v4Exception);
    }
}
