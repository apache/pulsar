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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.re2j.PatternSyntaxException;
import java.lang.ref.WeakReference;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class TopicsPatternCacheTest {
    private static final String REGEX = "tenant/ns/topic-.*";

    private final TopicsPatternCache topicsPatternCache = new TopicsPatternCache();

    @Test
    public void testReturnsPatternOfSameRegexAndImplementation() {
        TopicsPattern topicsPattern =
                topicsPatternCache.get(REGEX, TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK);
        assertThat(topicsPattern.matches("tenant/ns/topic-1")).isTrue();
        assertThat(topicsPattern.matches("tenant/ns/other-1")).isFalse();

        assertThat(topicsPatternCache.get(REGEX, TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK))
                .as("pattern of the same regex and implementation").isSameAs(topicsPattern);
        assertThat(topicsPatternCache.get(REGEX, TopicsPattern.RegexImplementation.JDK))
                .as("pattern of another implementation").isNotSameAs(topicsPattern);
        assertThat(topicsPatternCache.get("persistent://" + REGEX,
                TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK))
                .as("pattern of another input pattern").isNotSameAs(topicsPattern)
                .extracting(TopicsPattern::inputPattern).isEqualTo("persistent://" + REGEX);
    }

    @Test
    public void testDoesNotKeepPatternAliveAfterItsUsersAreGone() {
        WeakReference<TopicsPattern> topicsPattern = new WeakReference<>(
                topicsPatternCache.get("tenant/ns/unused-.*", TopicsPattern.RegexImplementation.RE2J));
        Awaitility.await().untilAsserted(() -> {
            System.gc();
            assertThat(topicsPattern.get()).as("pattern that only the cache references").isNull();
        });
    }

    @Test
    public void testDoesNotCacheMatchAllPattern() {
        // the regexes are distinct, but they all match all topics after removing the topic domain scheme
        for (int i = 0; i < 100; i++) {
            assertThat(topicsPatternCache.get("persistent://.*://alias-" + i,
                    TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK))
                    .isSameAs(TopicsPatternFactory.matchAll());
        }
        assertThat(topicsPatternCache.size()).isZero();
    }

    @Test
    public void testInvalidRegexIsNotCached() {
        // RE2J doesn't support lookaheads
        String regex = "tenant/ns/(?!excluded-).+";
        assertThatThrownBy(() -> topicsPatternCache.get(regex, TopicsPattern.RegexImplementation.RE2J))
                .isInstanceOf(PatternSyntaxException.class);
        assertThatThrownBy(() -> topicsPatternCache.get(regex, TopicsPattern.RegexImplementation.RE2J))
                .as("second attempt").isInstanceOf(PatternSyntaxException.class);
    }
}
