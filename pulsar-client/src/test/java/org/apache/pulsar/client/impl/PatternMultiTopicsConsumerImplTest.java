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
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.Sets;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class PatternMultiTopicsConsumerImplTest {

    private PatternMultiTopicsConsumerImpl.TopicsChangedListener mockListener;

    private Consumer<String> mockTopicsHashSetter;


    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        mockListener = mock(PatternMultiTopicsConsumerImpl.TopicsChangedListener.class);
        when(mockListener.onTopicsAdded(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockListener.onTopicsRemoved(any())).thenReturn(CompletableFuture.completedFuture(null));
        mockTopicsHashSetter = mock(Consumer.class);

    }

    @Test
    public void testChangedUnfilteredResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                Pattern.compile("tenant/my-ns/name-.*"),
                mockTopicsHashSetter,
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2",
                        "persistent://tenant/my-ns/non-matching"),
                        null, false, true),
                mockListener,
                Collections.emptyList());
        verify(mockListener).onTopicsAdded(Sets.newHashSet(
                "persistent://tenant/my-ns/name-1",
                "persistent://tenant/my-ns/name-2"));
        verify(mockListener).onTopicsRemoved(Collections.emptySet());
        verify(mockTopicsHashSetter).accept(null);
    }

    @Test
    public void testChangedFilteredResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                Pattern.compile("tenant/my-ns/name-.*"),
                mockTopicsHashSetter,
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-0",
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2"),
                        "TOPICS_HASH", true, true),
                mockListener,
                Arrays.asList("persistent://tenant/my-ns/name-0"));
        verify(mockListener).onTopicsAdded(Sets.newHashSet(
                "persistent://tenant/my-ns/name-1",
                "persistent://tenant/my-ns/name-2"));
        verify(mockListener).onTopicsRemoved(Collections.emptySet());
        verify(mockTopicsHashSetter).accept("TOPICS_HASH");
    }

    @Test
    public void testUnchangedResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                Pattern.compile("tenant/my-ns/name-.*"),
                mockTopicsHashSetter,
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-0",
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2"),
                        "TOPICS_HASH", true, false),
                mockListener,
                Arrays.asList("persistent://tenant/my-ns/name-0"));
        verify(mockListener, never()).onTopicsAdded(any());
        verify(mockListener, never()).onTopicsRemoved(any());
        verify(mockTopicsHashSetter).accept("TOPICS_HASH");
    }
}
