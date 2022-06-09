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
package org.apache.pulsar.broker.resources;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.util.function.BiConsumer;

public class TopicResourcesTest {

    private MetadataStore metadataStore;
    private TopicResources topicResources;

    @BeforeMethod
    public void setup() {
        metadataStore = mock(MetadataStore.class);
        topicResources = new TopicResources(metadataStore);
    }

    @Test
    public void testConstructorRegistersAsListener() {
        verify(metadataStore).registerListener(any());
    }

    @Test
    public void testListenerInvokedWhenTopicCreated() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/namespace/topic", NotificationType.Created);
    }

    @Test
    public void testListenerInvokedWhenTopicV1Created() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/cluster/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/cluster/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/cluster/namespace/topic", NotificationType.Created);
    }

    @Test
    public void testListenerInvokedWhenTopicDeleted() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Deleted, "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/namespace/topic", NotificationType.Deleted);
    }

    @Test
    public void testListenerNotInvokedWhenSubscriptionCreated() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/namespace/persistent/topic/subscription"));
        verifyNoInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicCreatedInOtherNamespace() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/namespace2/persistent/topic"));
        verifyNoInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicModified() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Modified, "/managed-ledgers/tenant/namespace/persistent/topic"));
        verifyNoInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedAfterDeregistered() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/namespace/topic", NotificationType.Created);
        topicResources.deregisterPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created, "/managed-ledgers/tenant/namespace/persistent/topic2"));
        verifyNoMoreInteractions(listener);
    }

}
