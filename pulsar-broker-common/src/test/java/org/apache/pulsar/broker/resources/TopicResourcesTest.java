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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicResourcesTest {

    private MetadataStoreExtended metadataStore;
    private TopicResources topicResources;

    @BeforeMethod
    public void setup() {
        metadataStore = mock(MetadataStoreExtended.class);
        topicResources = new TopicResources(metadataStore);
    }

    @Test
    public void testConstructorRegistersAsListener() {
        verify(metadataStore).registerListener(any());
    }

    @Test
    public void testListenerInvokedWhenTopicCreated() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
    }

    @Test
    public void testListenerInvokedWhenTopicDeleted() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Deleted,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Deleted);
    }

    @Test
    public void testListenerNotInvokedWhenSubscriptionCreated() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic/subscription"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicCreatedInOtherNamespace() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace2/persistent/topic"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicModified() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Modified,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedForSystemTopicChanges() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/__change_events"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedAfterDeregistered() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
        topicResources.deregisterPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic2"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerInvokedWithDecodedTopicName() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic%3Atest"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic:test", NotificationType.Created);
    }

    @Test
    public void testNamespaceContainsDotsShouldntMatchAny() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/name.pace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verifyNoMoreInteractions(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/name.pace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/name.pace/topic", NotificationType.Created);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testBiConsumerListenerNotInvokedAfterDeregistered() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/namespace/topic", NotificationType.Created);
        topicResources.deregisterPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic2"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListSegmentTopics() throws Exception {
        String segmentDomain = "/managed-ledgers/tenant/namespace/segment";
        when(metadataStore.getChildren(segmentDomain))
                .thenReturn(CompletableFuture.completedFuture(List.of("topic", "my%20topic")));
        when(metadataStore.getChildren(segmentDomain + "/topic"))
                .thenReturn(CompletableFuture.completedFuture(List.of("0000-7fff-1", "8000-ffff-2")));
        when(metadataStore.getChildren(segmentDomain + "/my%20topic"))
                .thenReturn(CompletableFuture.completedFuture(List.of("0000-ffff-0")));

        assertThat(topicResources.listSegmentTopicsAsync(NamespaceName.get("tenant/namespace")).get())
                .containsExactlyInAnyOrder(
                        "segment://tenant/namespace/topic/0000-7fff-1",
                        "segment://tenant/namespace/topic/8000-ffff-2",
                        "segment://tenant/namespace/my topic/0000-ffff-0");
    }
}
