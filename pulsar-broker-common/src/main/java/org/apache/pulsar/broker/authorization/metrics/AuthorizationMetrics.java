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
package org.apache.pulsar.broker.authorization.metrics;

import io.prometheus.client.Counter;

public final class AuthorizationMetrics {
    public static final String AUTHORIZATION_OPERATIONS_METRIC_NAME = "pulsar_authorization_operations_total";
    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_FAILURE = "failure";
    public static final String RESOURCE_TYPE_TOPIC_POLICY = "topic_policy";

    private static final Counter authorizationOperations = Counter.build()
            .name(AUTHORIZATION_OPERATIONS_METRIC_NAME)
            .help("Pulsar authorization operations")
            .labelNames("resource_type", "operation", "result")
            .register();

    private AuthorizationMetrics() {
    }

    public static void recordSuccess(String resourceType, String operation) {
        authorizationOperations.labels(resourceType, operation, RESULT_SUCCESS).inc();
    }

    public static void recordFailure(String resourceType, String operation) {
        authorizationOperations.labels(resourceType, operation, RESULT_FAILURE).inc();
    }
}
