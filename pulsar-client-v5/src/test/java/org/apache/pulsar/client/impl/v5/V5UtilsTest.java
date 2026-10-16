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
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
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

    /** A v4 exception, and the V5 type of its error category (error-model.md §2). */
    @DataProvider
    public static Object[][] errorCategories() {
        return new Object[][] {
            {new org.apache.pulsar.client.api.PulsarClientException.NotFoundException("error"),
                    PulsarClientException.NotFoundException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.TopicDoesNotExistException("error"),
                    PulsarClientException.NotFoundException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.AuthenticationException("error"),
                    PulsarClientException.AuthenticationException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException("error"),
                    PulsarClientException.GettingAuthenticationDataException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException("error"),
                    PulsarClientException.UnsupportedAuthenticationException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.AuthorizationException("error"),
                    PulsarClientException.AuthorizationException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.TimeoutException("error"),
                    PulsarClientException.TimeoutException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException("error"),
                    PulsarClientException.AlreadyClosedException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException("error"),
                    PulsarClientException.ProducerBusyException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ProducerFencedException("error"),
                    PulsarClientException.ProducerBusyException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ConsumerBusyException("error"),
                    PulsarClientException.ConsumerBusyException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ConsumerAssignException("error"),
                    PulsarClientException.ConsumerBusyException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException("error"),
                    PulsarClientException.IncompatibleSchemaException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.TopicTerminatedException("error"),
                    PulsarClientException.TopicTerminatedException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ConnectException("error"),
                    PulsarClientException.ConnectException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.NotConnectedException("error"),
                    PulsarClientException.NotConnectedException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.ProducerQueueIsFullError("error"),
                    PulsarClientException.ProducerQueueIsFullException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.MemoryBufferIsFullError("error"),
                    PulsarClientException.MemoryBufferIsFullException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.TransactionConflictException("error"),
                    PulsarClientException.TransactionConflictException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.TransactionHasOperationFailedException("error"),
                    PulsarClientException.TransactionConflictException.class},
            {new TransactionCoordinatorClientException.InvalidTxnStatusException("error"),
                    PulsarClientException.TransactionConflictException.class},
            {new TransactionCoordinatorClientException.TransactionNotFoundException("error"),
                    PulsarClientException.TransactionConflictException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.CryptoException("error"),
                    PulsarClientException.CryptoException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.InvalidConfigurationException("error"),
                    PulsarClientException.InvalidConfigurationException.class},
            {new org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL("error", null),
                    PulsarClientException.InvalidServiceURLException.class},
        };
    }

    @Test(dataProvider = "errorCategories")
    public void testToV5ExceptionKeepsTheErrorCategory(Exception v4Exception, Class<?> v5Type) {
        assertConverted(V5Utils.toV5Exception(v4Exception), v5Type, v4Exception);
        assertConverted(V5Utils.toV5Exception(new CompletionException(v4Exception)), v5Type, v4Exception);
        assertConverted(V5Utils.toV5Exception(new ExecutionException(v4Exception)), v5Type, v4Exception);
    }

    @Test
    public void testToV5ExceptionWrapsOtherFailuresInThePlainException() {
        var notAllowed = new org.apache.pulsar.client.api.PulsarClientException.NotAllowedException("not allowed");

        assertConverted(V5Utils.toV5Exception(new CompletionException(notAllowed)), PulsarClientException.class,
                notAllowed);
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
