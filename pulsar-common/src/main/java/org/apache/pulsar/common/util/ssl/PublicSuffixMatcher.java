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
package org.apache.pulsar.common.util.ssl;

import java.net.IDN;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class that can test if DNS names match the content of the Public Suffix List.
 * <p>
 * An up-to-date list of suffixes can be obtained from
 * <a href="http://publicsuffix.org/">publicsuffix.org</a>
 *
 * Copied from httpclient.
 */
public final class PublicSuffixMatcher {

    private final Map<String, DomainType> rules;
    private final Map<String, DomainType> exceptions;

    public PublicSuffixMatcher(final Collection<String> rules, final Collection<String> exceptions) {
        this(DomainType.UNKNOWN, rules, exceptions);
    }

    public PublicSuffixMatcher(final DomainType domainType,
                               final Collection<String> rules, final Collection<String> exceptions) {
        if (domainType == null) {
            throw new IllegalArgumentException("Domain type is null");
        }
        if (rules == null) {
            throw new IllegalArgumentException("Domain suffix rules are null");
        }
        this.rules = new ConcurrentHashMap<String, DomainType>(rules.size());
        for (String rule: rules) {
            this.rules.put(rule, domainType);
        }
        this.exceptions = new ConcurrentHashMap<String, DomainType>();
        if (exceptions != null) {
            for (String exception: exceptions) {
                this.exceptions.put(exception, domainType);
            }
        }
    }

    public PublicSuffixMatcher(final Collection<PublicSuffixList> lists) {
        if (lists == null) {
            throw new IllegalArgumentException("Domain suffix lists are null");
        }
        this.rules = new ConcurrentHashMap<String, DomainType>();
        this.exceptions = new ConcurrentHashMap<String, DomainType>();
        for (PublicSuffixList list: lists) {
            final DomainType domainType = list.getType();
            for (String rule: list.getRules()) {
                this.rules.put(rule, domainType);
            }
            final List<String> listExceptions = list.getExceptions();
            if (listExceptions != null) {
                for (String exception: listExceptions) {
                    this.exceptions.put(exception, domainType);
                }
            }
        }
    }

    private static boolean hasEntry(final Map<String, DomainType> map, final String rule,
                                    final DomainType expectedType) {
        if (map == null) {
            return false;
        }
        final DomainType domainType = map.get(rule);
        if (domainType == null) {
            return false;
        }
        return expectedType == null || domainType.equals(expectedType);
    }

    private boolean hasRule(final String rule, final DomainType expectedType) {
        return hasEntry(this.rules, rule, expectedType);
    }

    private boolean hasException(final String exception, final DomainType expectedType) {
        return hasEntry(this.exceptions, exception, expectedType);
    }

    /**
     * Returns registrable part of the domain for the given domain name or {@code null}
     * if given domain represents a public suffix.
     *
     * @param domain
     * @return domain root
     */
    public String getDomainRoot(final String domain) {
        return getDomainRoot(domain, null);
    }

    /**
     * Returns registrable part of the domain for the given domain name or {@code null}
     * if given domain represents a public suffix.
     *
     * @param domain
     * @param expectedType expected domain type or {@code null} if any.
     * @return domain root
     */
    public String getDomainRoot(final String domain, final DomainType expectedType) {
        if (domain == null) {
            return null;
        }
        if (domain.startsWith(".")) {
            return null;
        }
        String domainName = null;
        String segment = domain.toLowerCase(Locale.ROOT);
        while (segment != null) {

            // An exception rule takes priority over any other matching rule.
            if (hasException(IDN.toUnicode(segment), expectedType)) {
                return segment;
            }

            if (hasRule(IDN.toUnicode(segment), expectedType)) {
                break;
            }

            final int nextdot = segment.indexOf('.');
            final String nextSegment = nextdot != -1 ? segment.substring(nextdot + 1) : null;

            if (nextSegment != null
                && hasRule("*." + IDN.toUnicode(nextSegment), expectedType)) {
                break;
            }
            if (nextdot != -1) {
                domainName = segment;
            }
            segment = nextSegment;
        }
        return domainName;
    }

    /**
     * Tests whether the given domain matches any of entry from the public suffix list.
     */
    public boolean matches(final String domain) {
        return matches(domain, null);
    }

    /**
     * Tests whether the given domain matches any of entry from the public suffix list.
     *
     * @param domain
     * @param expectedType expected domain type or {@code null} if any.
     * @return {@code true} if the given domain matches any of the public suffixes.
     */
    public boolean matches(final String domain, final DomainType expectedType) {
        if (domain == null) {
            return false;
        }
        final String domainRoot = getDomainRoot(
                domain.startsWith(".") ? domain.substring(1) : domain, expectedType);
        return domainRoot == null;
    }

    public enum DomainType {
        UNKNOWN, ICANN, PRIVATE
    }

    public static final class PublicSuffixList {

        private final DomainType type;
        private final List<String> rules;
        private final List<String> exceptions;

        public PublicSuffixList(final DomainType type, final List<String> rules, final List<String> exceptions) {
            if (type == null) {
                throw new IllegalArgumentException("Domain type is null");
            }
            if (rules == null) {
                throw new IllegalArgumentException("Domain suffix rules are null");
            }
            if (exceptions == null) {
                throw new IllegalArgumentException("Domain suffix exceptions are null");
            }
            this.type = type;
            this.rules = Collections.unmodifiableList(rules);
            this.exceptions = Collections.unmodifiableList(exceptions);
        }

        public DomainType getType() {
            return type;
        }

        public List<String> getRules() {
            return rules;
        }

        public List<String> getExceptions() {
            return exceptions;
        }

    }
    
}
