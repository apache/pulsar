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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pulsar.broker.resources.ScalableTopicResources.MetadataPathListener;
import org.apache.pulsar.broker.resources.ScalableTopicResources.NamespaceListener;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ScalableTopicResourcesTest {
    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/topic");

    @DataProvider
    public Object[][] metadataStoreTypes() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testMetadataListenerReceivesTopicAndNestedNotifications(boolean extended) {
        Fixture fixture = fixture(extended);
        List<Notification> received = new ArrayList<>();
        fixture.resources().registerMetadataListener(received::add);
        String topicPath = fixture.resources().topicPath(TOPIC);
        List<Notification> notifications = List.of(
                new Notification(NotificationType.Created, topicPath),
                new Notification(NotificationType.Modified, fixture.resources().controllerLockPath(TOPIC)),
                new Notification(NotificationType.Deleted, topicPath + "/subscriptions/sub"),
                new Notification(NotificationType.Created, topicPath + "/subscriptions/sub/consumers/consumer"),
                new Notification(NotificationType.Modified, topicPath + "/segments/0/load"));

        notifications.forEach(fixture.storeListener());

        assertThat(received).as("all notifications below /topics/, including nested paths")
                .containsExactlyElementsOf(notifications);
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testMetadataListenerExcludesOtherPaths(boolean extended) {
        Fixture fixture = fixture(extended);
        List<Notification> received = new ArrayList<>();
        fixture.resources().registerMetadataListener(received::add);
        for (String path : List.of("/topics", "/topics-other/tenant/ns/topic", "/other/topics/topic",
                "/admin/policies/tenant/ns", "/loadbalance/brokers/broker")) {
            fixture.storeListener().accept(new Notification(NotificationType.Modified, path));
        }
        assertThat(received).as("only descendants of /topics/ are observed").isEmpty();
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testMetadataListenerRegistrationIsIdempotent(boolean extended) {
        Fixture fixture = fixture(extended);
        List<Notification> received = new ArrayList<>();
        Consumer<Notification> listener = received::add;
        fixture.resources().registerMetadataListener(listener);
        fixture.resources().registerMetadataListener(listener);
        Notification notification = new Notification(NotificationType.Modified, fixture.resources().topicPath(TOPIC));

        fixture.storeListener().accept(notification);

        assertThat(received).as("one delivery per registered observer").containsExactly(notification);
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testDeregisterMetadataListenerStopsNotifications(boolean extended) {
        Fixture fixture = fixture(extended);
        List<Notification> received = new ArrayList<>();
        Consumer<Notification> listener = received::add;
        Notification notification = new Notification(NotificationType.Modified, fixture.resources().topicPath(TOPIC));
        fixture.resources().registerMetadataListener(listener);
        fixture.storeListener().accept(notification);
        fixture.resources().deregisterMetadataListener(listener);
        fixture.resources().deregisterMetadataListener(listener);
        fixture.storeListener().accept(notification);
        assertThat(received).as("deregistering twice is safe and removes the observer").containsExactly(notification);

        fixture.resources().registerMetadataListener(listener);
        fixture.storeListener().accept(notification);
        assertThat(received).as("a deregistered observer can be registered again")
                .containsExactly(notification, notification);
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testMetadataObserverFailureIsIsolatedFromOtherListeners(boolean extended) {
        Fixture fixture = fixture(extended);
        String topicPath = fixture.resources().topicPath(TOPIC);
        Notification topic = new Notification(NotificationType.Modified, topicPath);
        Notification controller = new Notification(NotificationType.Deleted,
                fixture.resources().controllerLockPath(TOPIC));
        MetadataPathListener pathListener = mock(MetadataPathListener.class);
        when(pathListener.getMetadataPath()).thenReturn(topicPath);
        NamespaceListener namespaceListener = mock(NamespaceListener.class);
        when(namespaceListener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/ns"));
        fixture.resources().registerPathListener(pathListener);
        fixture.resources().registerNamespaceListener(namespaceListener);
        AtomicInteger failedCalls = new AtomicInteger();
        Consumer<Notification> failing = notification -> {
            failedCalls.incrementAndGet();
            throw new IllegalStateException("observer failed");
        };
        List<Notification> received = new ArrayList<>();
        fixture.resources().registerMetadataListener(failing);
        fixture.resources().registerMetadataListener(received::add);

        fixture.storeListener().accept(topic);
        fixture.storeListener().accept(controller);

        assertThat(failedCalls.get()).as("the failing observer receives both topic and controller events").isEqualTo(2);
        assertThat(received).as("a failing observer does not suppress other observers")
                .containsExactly(topic, controller);
        verify(pathListener).onNotification(topic);
        verify(pathListener, never()).onNotification(controller);
        verify(namespaceListener).onNotification(topic);
        verify(namespaceListener, never()).onNotification(controller);

        fixture.resources().deregisterMetadataListener(failing);
        fixture.storeListener().accept(topic);
        assertThat(failedCalls.get()).isEqualTo(2);
        verify(pathListener, times(2)).onNotification(topic);
        verify(namespaceListener, times(2)).onNotification(topic);
    }

    @Test(dataProvider = "metadataStoreTypes")
    public void testMetadataObserversReuseConstructorStoreListener(boolean extended) {
        Fixture fixture = fixture(extended);
        verify(fixture.store()).registerCancellableListener(any());
        Consumer<Notification> first = notification -> { };
        Consumer<Notification> second = notification -> { };
        fixture.resources().registerMetadataListener(first);
        fixture.resources().registerMetadataListener(first);
        fixture.resources().registerMetadataListener(second);
        fixture.resources().deregisterMetadataListener(first);
        fixture.resources().deregisterMetadataListener(second);

        verify(fixture.store(), times(1)).registerCancellableListener(any());
    }

    private static Fixture fixture(boolean extended) {
        MetadataStore store = extended ? mock(MetadataStoreExtended.class) : mock(MetadataStore.class);
        AtomicReference<Consumer<Notification>> storeListener = new AtomicReference<>();
        Runnable cancelStoreListener = mock(Runnable.class);
        doAnswer(invocation -> {
            storeListener.set(invocation.getArgument(0));
            return cancelStoreListener;
        }).when(store).registerCancellableListener(any());
        ScalableTopicResources resources = new ScalableTopicResources(store, 30);
        assertThat(storeListener.get()).as("the constructor installs the shared notification dispatcher").isNotNull();
        return new Fixture(store, resources, storeListener.get());
    }

    private record Fixture(MetadataStore store, ScalableTopicResources resources,
                           Consumer<Notification> storeListener) { }
}
