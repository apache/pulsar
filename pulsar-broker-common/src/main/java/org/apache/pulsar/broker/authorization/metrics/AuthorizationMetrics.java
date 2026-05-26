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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.prometheus.client.Counter;

public class AuthorizationMetrics {
    public static final String AUTHORIZATION_OPERATIONS_METRIC_NAME = "pulsar_authorization_operations_total";
    public static final String AUTHORIZATION_COUNTER_METRIC_NAME = "pulsar.authorization.operation.count";
    public static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.pulsar.authorization";
    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_FAILURE = "failure";
    public static final String RESULT_ERROR = "error";
    public static final String RESOURCE_TYPE_SUPERUSER = "superuser";
    public static final String RESOURCE_TYPE_TENANT_ADMIN = "tenant_admin";
    public static final String RESOURCE_TYPE_TENANT = "tenant";
    public static final String RESOURCE_TYPE_BROKER = "broker";
    public static final String RESOURCE_TYPE_CLUSTER = "cluster";
    public static final String RESOURCE_TYPE_CLUSTER_POLICY = "cluster_policy";
    public static final String RESOURCE_TYPE_NAMESPACE = "namespace";
    public static final String RESOURCE_TYPE_NAMESPACE_POLICY = "namespace_policy";
    public static final String RESOURCE_TYPE_TOPIC = "topic";
    public static final String RESOURCE_TYPE_TOPIC_POLICY = "topic_policy";
    public static final AttributeKey<String> RESOURCE_TYPE_KEY =
            AttributeKey.stringKey("pulsar.authorization.resource.type");
    public static final AttributeKey<String> OPERATION_KEY = AttributeKey.stringKey("pulsar.authorization.operation");
    public static final AttributeKey<String> RESULT_KEY = AttributeKey.stringKey("pulsar.authorization.result");

    private static final Counter authorizationOperations = Counter.build()
            .name(AUTHORIZATION_OPERATIONS_METRIC_NAME)
            .help("Pulsar authorization operations")
            .labelNames("resource_type", "operation", "result")
            .register();

    private final LongCounter authorizationCounter;

    public AuthorizationMetrics(OpenTelemetry openTelemetry) {
        var meter = openTelemetry.getMeter(INSTRUMENTATION_SCOPE_NAME);
        authorizationCounter = meter.counterBuilder(AUTHORIZATION_COUNTER_METRIC_NAME)
                .setDescription("The number of authorization operations")
                .setUnit("{operation}")
                .build();
    }

    public void recordSuccess(String resourceType, String operation) {
        record(resourceType, operation, RESULT_SUCCESS);
    }

    public void recordFailure(String resourceType, String operation) {
        record(resourceType, operation, RESULT_FAILURE);
    }

    public void recordError(String resourceType, String operation) {
        record(resourceType, operation, RESULT_ERROR);
    }

    private void record(String resourceType, String operation, String result) {
        authorizationOperations.labels(resourceType, operation, result).inc();
        authorizationCounter.add(1, Attributes.of(RESOURCE_TYPE_KEY, resourceType,
                OPERATION_KEY, operation,
                RESULT_KEY, result));
    }
}
