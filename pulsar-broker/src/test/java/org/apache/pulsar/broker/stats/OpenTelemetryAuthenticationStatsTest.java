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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import io.jsonwebtoken.SignatureAlgorithm;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OpenTelemetryAuthenticationStatsTest extends BrokerTestBase {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
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
    public void testMetrics() throws IOException, AuthenticationException {
        var secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        @Cleanup
        var provider = new AuthenticationProviderToken();

        var properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        var authenticationMetrics = WhiteboxImpl.getByNameAndType(pulsar.getBrokerService(), "authenticationMetrics", AuthenticationMetrics.class);

        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setProperties(properties);
        var initParameters = AuthenticationProvider.InitParameters.builder()
                .config(conf)
                .authenticationMetrics(authenticationMetrics)
                .build();
        provider.initialize(initParameters);

        // Authentication should fail if not credentials passed.
        assertThatThrownBy(() -> provider.authenticate(new AuthenticationDataSource() {}))
                .isInstanceOf(AuthenticationException.class);
        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthenticationMetrics.AUTHENTICATION_COUNTER_METRIC_NAME,
                Attributes.empty(),
                1);

        // Pulsar protocol auth
        var token = AuthTokenUtils.createToken(secretKey, "subject", Optional.empty());
        provider.authenticate(new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromCommand() {
                return true;
            }

            @Override
            public String getCommandData() {
                return token;
            }
        });

        assertMetricLongSumValue(pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics(),
                AuthenticationMetrics.AUTHENTICATION_COUNTER_METRIC_NAME,
                Attributes.empty(),
                1);
    }
}
