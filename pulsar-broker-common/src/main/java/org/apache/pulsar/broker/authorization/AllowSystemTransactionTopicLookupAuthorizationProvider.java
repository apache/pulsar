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
package org.apache.pulsar.broker.authorization;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Authorization implementation that allows clients to use transactions even
 * if they don't have lookup permissions for the system tenant.
 */
public class AllowSystemTransactionTopicLookupAuthorizationProvider extends PulsarAuthorizationProvider {

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                     AuthenticationDataSource authenticationData) {
        if (SystemTopicNames.isTransactionCoordinatorAssign(topicName)) {
            return CompletableFuture.completedFuture(true);
        }
        return super.canLookupAsync(topicName, role, authenticationData);
    }
}
