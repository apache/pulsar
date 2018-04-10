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
package org.apache.pulsar.common.naming;

import org.apache.commons.lang3.StringUtils;

/**
 * Encapsulate the parsing of the short topic name.
 */
public final class TopicNames {

    public static final String PUBLIC_PROPERTY = "public";
    public static final String DEFAULT_NAMESPACE = "default";

    /**
     * Get the fully qualified topic name <tt>topicName</tt>.
     *
     * @param topicName topic name - it can be a fully qualified topic name or a short topic name.
     * @return fully qualified topic name
     */
    public static TopicName getFullyQualifiedTopicName(String topicName) {

        if (topicName.contains("://")) {
            return TopicName.get(topicName);
        } else {
            // Short Name can be:
            // 1) property/namespace/topic
            // 2) topic

            String[] parts = StringUtils.split(topicName, '/');
            if (parts.length == 3) {
                return TopicName.get(
                    TopicDomain.persistent.value(), parts[0], parts[1], parts[2]);
            } else if (parts.length == 1) {
                return TopicName.get(
                    TopicDomain.persistent.value(), PUBLIC_PROPERTY, DEFAULT_NAMESPACE, parts[0]);
            } else {
                throw new IllegalArgumentException(
                    "Invalid short topic name '" + topicName + "', it should be in the format of "
                    + "<tenant>/<namespace>/<topic> or <topic>");
            }
        }

    }

    private TopicNames() {}

}
