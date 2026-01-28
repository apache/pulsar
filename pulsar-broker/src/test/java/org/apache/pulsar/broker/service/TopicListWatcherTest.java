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
package org.apache.pulsar.broker.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.apache.pulsar.metadata.api.NotificationType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicListWatcherTest {

    private static final List<String> INITIAL_TOPIC_LIST = Arrays.asList(
            "persistent://tenant/ns/topic1",
            "persistent://tenant/ns/topic2",
            "persistent://tenant/ns/t3"
    );

    private static final long ID = 7;
    private static final TopicsPattern PATTERN =
            TopicsPatternFactory.create("tenant/ns/topic\\d+", TopicsPattern.RegexImplementation.RE2J);


    private TopicListService topicListService;
    private TopicListService.TopicListWatcher watcher;



    @BeforeMethod(alwaysRun = true)
    public void setup() {
        topicListService = mock(TopicListService.class);
        watcher = new TopicListService.TopicListWatcher(topicListService, ID, NamespaceName.get("tenant", "ns"),
                PATTERN, INITIAL_TOPIC_LIST, MoreExecutors.directExecutor(), 9);
        watcher.sendingCompleted();
    }

    @Test
    public void testGetMatchingTopicsReturnsFilteredList() {
        Assert.assertEquals(
                Arrays.asList("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2"),
                watcher.getMatchingTopics());
    }

    @Test
    public void testAcceptSendsNotificationAndRemembersTopic() {
        String newTopic = "persistent://tenant/ns/topic3";
        watcher.onTopicEvent(newTopic, NotificationType.Created);

        List<String> allMatchingTopics = Arrays.asList(
                "persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2", newTopic);
        String hash = TopicList.calculateHash(allMatchingTopics);
        verify(topicListService).sendTopicListUpdate(eq(ID), eq(hash), eq(Collections.emptyList()),
                eq(Collections.singletonList(newTopic)), any());
        Assert.assertEquals(
                allMatchingTopics,
                watcher.getMatchingTopics());
    }

    @Test
    public void testAcceptSendsNotificationAndForgetsTopic() {
        String deletedTopic = "persistent://tenant/ns/topic1";
        watcher.onTopicEvent(deletedTopic, NotificationType.Deleted);

        List<String> allMatchingTopics = Collections.singletonList("persistent://tenant/ns/topic2");
        String hash = TopicList.calculateHash(allMatchingTopics);
        verify(topicListService).sendTopicListUpdate(eq(ID), eq(hash),
                eq(Collections.singletonList(deletedTopic)), eq(Collections.emptyList()), any());
        Assert.assertEquals(
                allMatchingTopics,
                watcher.getMatchingTopics());
    }

    @Test
    public void testAcceptIgnoresNonMatching() {
        watcher.onTopicEvent("persistent://tenant/ns/mytopic", NotificationType.Created);
        verifyNoInteractions(topicListService);
        Assert.assertEquals(
                Arrays.asList("persistent://tenant/ns/topic1", "persistent://tenant/ns/topic2"),
                watcher.getMatchingTopics());
    }

    @Test
    public void testUpdateQueueOverFlowPerformsFullUpdate() {
        for (int i = 10; i <= 20; i++) {
            String newTopic = "persistent://tenant/ns/topic" + i;
            watcher.onTopicEvent(newTopic, NotificationType.Created);
        }
        verify(topicListService).sendTopicListUpdate(anyLong(), anyString(), any(), any(), any());
        verify(topicListService).updateTopicListWatcher(any(), eq(null), eq(null));
        verifyNoMoreInteractions(topicListService);
    }
}
