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

/**
 * From Apache HTTP client
 */

package org.apache.pulsar.common.tls;

import java.util.Collections;
import java.util.List;

import lombok.Data;

/**
 * Public suffix is a set of DNS names or wildcards concatenated with dots. It represents the part of a domain name
 * which is not under the control of the individual registrant
 * <p>
 * An up-to-date list of suffixes can be obtained from <a href="http://publicsuffix.org/">publicsuffix.org</a>
 *
 * @since 4.4
 */
@Data
public final class PublicSuffixList {

    private final DomainType type;
    private final List<String> rules;
    private final List<String> exceptions;

    /**
     * @since 4.5
     */
    public PublicSuffixList(final DomainType type, final List<String> rules, final List<String> exceptions) {
        this.type = type;
        this.rules = Collections.unmodifiableList(rules);
        this.exceptions = Collections
                .unmodifiableList(exceptions != null ? exceptions : Collections.<String> emptyList());
    }

    public PublicSuffixList(final List<String> rules, final List<String> exceptions) {
        this(DomainType.UNKNOWN, rules, exceptions);
    }
}
