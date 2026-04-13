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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import io.opentelemetry.api.common.Attributes;
import java.util.HashSet;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.authorization.MockAuthorizationProvider;
import org.apache.pulsar.broker.authorization.metrics.AuthorizationMetrics;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryAuthorizationStatsTest extends BrokerTestBase {

    private AuthorizationService authorizationService;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        HashSet<String> proxyRoles = new HashSet<>();
        proxyRoles.add("pass.proxy");
        proxyRoles.add("fail.proxy");
        conf.setProxyRoles(proxyRoles);
        authorizationService = new AuthorizationService(conf, null, pulsar.getOpenTelemetry().getOpenTelemetry());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder builder) {
        super.customizeMainPulsarTestContextBuilder(builder);
        builder.enableOpenTelemetry(true);
    }

    @Test
    public void testAuthorizationSuccess() throws Exception {
        authorizationService.allowTopicOperationAsync(TopicName.get("topic"),
                TopicOperation.PRODUCE, null, "pass.client", null).get();

        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthorizationMetrics.AUTHORIZATION_COUNTER_METRIC_NAME,
                Attributes.of(AuthorizationMetrics.RESOURCE_TYPE_KEY, "topic",
                        AuthorizationMetrics.OPERATION_KEY, "produce",
                        AuthorizationMetrics.RESULT_KEY, AuthorizationMetrics.RESULT_SUCCESS),
                1);
    }

    @Test
    public void testAuthorizationFailure() throws Exception {
        authorizationService.allowTopicOperationAsync(TopicName.get("topic"),
                TopicOperation.PRODUCE, null, "fail.client", null).get();

        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthorizationMetrics.AUTHORIZATION_COUNTER_METRIC_NAME,
                Attributes.of(AuthorizationMetrics.RESOURCE_TYPE_KEY, "topic",
                        AuthorizationMetrics.OPERATION_KEY, "produce",
                        AuthorizationMetrics.RESULT_KEY, AuthorizationMetrics.RESULT_FAILURE),
                1);
    }

    @Test
    public void testAuthorizationFailureForInvalidOriginalPrincipal() throws Exception {
        authorizationService.allowNamespaceOperationAsync(NamespaceName.get("public/default"),
                NamespaceOperation.PACKAGES, "pass.client", "pass.not-proxy", null).get();

        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthorizationMetrics.AUTHORIZATION_COUNTER_METRIC_NAME,
                Attributes.of(AuthorizationMetrics.RESOURCE_TYPE_KEY, "namespace",
                        AuthorizationMetrics.OPERATION_KEY, "packages",
                        AuthorizationMetrics.RESULT_KEY, AuthorizationMetrics.RESULT_FAILURE),
                1);
    }
}
