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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
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
    public void testListenerInvokedWhenTopicV1Created() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/cluster/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/cluster/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/cluster/namespace/topic", NotificationType.Created);
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
}
