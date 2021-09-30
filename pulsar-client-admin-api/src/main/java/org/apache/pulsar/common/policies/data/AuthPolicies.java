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
package org.apache.pulsar.common.policies.data;

import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.admin.utils.ReflectionUtils;

/**
 * Authentication policies.
 */
public interface AuthPolicies {
    Map<String, Set<AuthAction>> getNamespaceAuthentication();
    Map<String, Map<String, Set<AuthAction>>> getTopicAuthentication();
    Map<String, Set<String>> getSubscriptionAuthentication();

    static Builder builder() {
        return ReflectionUtils.newBuilder("org.apache.pulsar.client.admin.internal.data.AuthPoliciesImpl");
    }

    interface Builder {
        AuthPolicies build();
        Builder namespaceAuthentication(Map<String, Set<AuthAction>> namespaceAuthentication);
        Builder topicAuthentication(Map<String, Map<String, Set<AuthAction>>> topicAuthentication);
        Builder subscriptionAuthentication(Map<String, Set<String>> subscriptionAuthentication);
    }
}
