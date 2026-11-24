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

import java.util.regex.Pattern;

class JDKTopicsPattern implements TopicsPattern {
    private final String inputPattern;
    private final Pattern pattern;

    public JDKTopicsPattern(String inputPattern, String regexWithoutTopicDomainScheme) {
        this.inputPattern = inputPattern;
        this.pattern = Pattern.compile(regexWithoutTopicDomainScheme);
    }

    public JDKTopicsPattern(Pattern pattern) {
        this.pattern = pattern;
        this.inputPattern = pattern.pattern();
    }

    @Override
    public boolean matches(String topicName) {
        return pattern.matcher(topicName).matches();
    }

    @Override
    public String inputPattern() {
        return inputPattern;
    }
}
