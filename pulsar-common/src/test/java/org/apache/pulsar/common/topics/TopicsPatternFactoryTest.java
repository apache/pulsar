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
package org.apache.pulsar.common.topics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.google.re2j.PatternSyntaxException;
import java.util.regex.Pattern;
import org.testng.annotations.Test;

public class TopicsPatternFactoryTest {
    public static final String DEFAULT_TOPICS_PATTERN = "tenant/namespace/test-topic-.*";
    public static final String DEFAULT_TOPICS_PATTERN_WITH_DOMAIN = "persistent://" + DEFAULT_TOPICS_PATTERN;

    @Test
    public void testCreateWithJdkPattern() {
        Pattern jdkPattern = Pattern.compile(DEFAULT_TOPICS_PATTERN);
        TopicsPattern topicsPattern = TopicsPatternFactory.create(jdkPattern);
        assertTopicsPattern(topicsPattern, false);
    }

    private static void assertTopicsPattern(TopicsPattern topicsPattern, boolean inputIncludesDomainScheme) {
        assertNotNull(topicsPattern);
        assertEquals(topicsPattern.inputPattern(),
                inputIncludesDomainScheme ? DEFAULT_TOPICS_PATTERN_WITH_DOMAIN : DEFAULT_TOPICS_PATTERN);
        assertTopicsPatternMatches(topicsPattern);
    }

    private static void assertTopicsPatternMatches(TopicsPattern topicsPattern) {
        assertTrue(topicsPattern.matches("tenant/namespace/test-topic-1"));
        assertTrue(topicsPattern.matches("tenant/namespace/test-topic-abc"));
        assertFalse(topicsPattern.matches("tenant/namespace/other-topic"));
    }

    @Test
    public void testCreateWithJdkPatternContainingTopicDomainScheme() {
        Pattern jdkPattern = Pattern.compile(DEFAULT_TOPICS_PATTERN_WITH_DOMAIN);
        TopicsPattern topicsPattern = TopicsPatternFactory.create(jdkPattern);
        assertTopicsPattern(topicsPattern, true);
    }

    @Test
    public void testCreateWithStringAndRE2JImplementation() {
        TopicsPattern topicsPattern = TopicsPatternFactory.create(DEFAULT_TOPICS_PATTERN,
                TopicsPattern.RegexImplementation.RE2J);
        assertTopicsPattern(topicsPattern, false);
    }

    @Test
    public void testCreateWithStringAndJDKImplementation() {
        TopicsPattern topicsPattern = TopicsPatternFactory.create(DEFAULT_TOPICS_PATTERN,
                TopicsPattern.RegexImplementation.JDK);
        assertTopicsPattern(topicsPattern, false);
    }

    @Test
    public void testCreateWithStringAndRE2JWithJDKFallbackImplementation() {
        TopicsPattern topicsPattern = TopicsPatternFactory.create(DEFAULT_TOPICS_PATTERN,
                TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK);
        assertTopicsPattern(topicsPattern, false);
    }

    @Test
    public void testCreateWithStringContainingTopicDomainScheme() {
        TopicsPattern topicsPattern = TopicsPatternFactory.create(DEFAULT_TOPICS_PATTERN_WITH_DOMAIN,
                TopicsPattern.RegexImplementation.JDK);
        assertTopicsPattern(topicsPattern, true);
    }

    @Test
    public void testCreateWithAllTopicsPattern() {
        TopicsPattern topicsPattern = TopicsPatternFactory.create(".*",
                TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK);
        assertNotNull(topicsPattern);
        assertTrue(topicsPattern.matches("tenant/namespace/any-topic"));
        assertTrue(topicsPattern.matches("tenant/namespace/test-topic-1"));
        assertTrue(topicsPattern.matches(""));
    }

    @Test
    public void testMatchAll() {
        TopicsPattern topicsPattern = TopicsPatternFactory.matchAll();
        assertNotNull(topicsPattern);
        assertTrue(topicsPattern.matches("tenant/namespace/any-topic"));
        assertTrue(topicsPattern.matches("tenant/namespace/test-topic-1"));
        assertTrue(topicsPattern.matches(""));
        assertTrue(topicsPattern.matches("tenant/namespace/very-long-topic-name"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateWithInvalidImplementation() {
        TopicsPatternFactory.create(null, TopicsPattern.RegexImplementation.JDK);
    }

    @Test
    public void testRE2JFallbackToJDK() {
        // example of excluding topics that start with "test2-" or "test3-" or "other-", but require that the topic
        // name ends with "-topic" or "-topic-" and any suffix
        // This regex cannot be parsed by RE2J, so it should fall back to JDK regex
        String complexRegex = "tenant/namespace/(?!(test2|test3|other)-).+-topic(-.*)?";
        TopicsPattern topicsPattern = TopicsPatternFactory.create(complexRegex,
                TopicsPattern.RegexImplementation.RE2J_WITH_JDK_FALLBACK);
        assertNotNull(topicsPattern);
        assertTopicsPatternMatches(topicsPattern);
        assertTrue(topicsPattern.matches("tenant/namespace/any-topic"));
        assertFalse(topicsPattern.matches("tenant/namespace/test2-topic"));
        assertFalse(topicsPattern.matches("tenant/namespace/test3-topic"));
        assertTrue(topicsPattern.matches("tenant/namespace/test4-topic"));
        assertTrue(topicsPattern.matches("tenant/namespace/test20-topic"));
    }

    @Test(expectedExceptions = PatternSyntaxException.class)
    public void testRE2JParsingFailure() {
        String complexRegex = "tenant/namespace/(?!(test2|test3|other)-).+-topic(-.*)?";
        TopicsPattern topicsPattern = TopicsPatternFactory.create(complexRegex,
                TopicsPattern.RegexImplementation.RE2J);
    }
}