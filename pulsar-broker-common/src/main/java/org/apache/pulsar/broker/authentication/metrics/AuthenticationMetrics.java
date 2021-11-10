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
package org.apache.pulsar.broker.authentication.metrics;

import io.prometheus.client.Counter;

public class AuthenticationMetrics {
    private static final Counter authSuccessMetrics = Counter.build()
            .name("pulsar_authentication_success_count")
            .help("Pulsar authentication success")
            .labelNames("provider_name", "auth_method")
            .register();
    private static final Counter authFailuresMetrics = Counter.build()
            .name("pulsar_authentication_failures_count")
            .help("Pulsar authentication failures")
            .labelNames("provider_name", "auth_method", "reason")
            .register();

    private static final Counter clientCertSelfSignedMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_self_signed_count")
            .help("Pulsar client authenticating with a TLS cert that is self-signed")
            .register();

    private static final Counter clientCertSmallRsaKeySizeMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_small_rsa_key_count")
            .help("Pulsar client authenticating with a TLS cert that has a small rsa key")
            .register();

    private static final Counter clientCertReachedWarnThresholdMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_expiration_soon_count")
            .help("Pulsar client authenticating with a TLS cert nearing expiration")
            .register();

    private static final Counter clientCertReachedErrorThresholdMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_expiration_imminent_count")
            .help("Pulsar client authenticating with a TLS cert whose expiration is imminent")
            .register();

    private static final Counter clientCertValidityDurationExceedsThresholdMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_validity_too_long_count")
            .help("Pulsar client authenticating with a TLS cert whose validity duration exceeds the system's configured (soft) maximum value")
            .register();

    private static final Counter clientCertWildcardMetrics = Counter.build()
            .name("pulsar_authentication_tls_cert_wildcard_count")
            .help("Pulsar client authenticating with a TLS cert that has wildcard CN")
            .register();

    /**
     * Log authenticate success event to the authentication metrics
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name
     */
    public static void authenticateSuccess(String providerName, String authMethod) {
        authSuccessMetrics.labels(providerName, authMethod).inc();
    }

    /**
     * Log authenticate failure event to the authentication metrics.
     * @param providerName The short class name of the provider
     * @param authMethod Authentication method name.
     * @param reason Failure reason.
     */
    public static void authenticateFailure(String providerName, String authMethod, String reason) {
        authFailuresMetrics.labels(providerName, authMethod, reason).inc();
    }

    /**
     * Log self-signed certificate encountered event to the authentication metrics.
     */
    public static void encounteredSelfSignedCertificate() {
        clientCertSelfSignedMetrics.inc();
    }

    /**
     * Log small RSA key size certificate encountered event to the authentication metrics.
     */
    public static void encounteredSmallRsaKeySize() {
        clientCertSmallRsaKeySizeMetrics.inc();
    }

    /**
     * Log certificate with warning expiration encountered event to the authentication metrics.
     */
    public static void encounteredCertificateUnderWarnThreshold() {
        clientCertReachedWarnThresholdMetrics.inc();
    }

    /**
     * Log certificate with error expiration encountered event to the authentication metrics.
     */
    public static void encounteredCertificateUnderErrorThreshold() {
        clientCertReachedErrorThresholdMetrics.inc();
    }

    /**
     * Log long validity certificate encountered event to the authentication metrics.
     */
    public static void encounteredLongValidityCertificate() {
        clientCertValidityDurationExceedsThresholdMetrics.inc();
    }

    /**
     * Log wildcard certificate encountered event to the authentication metrics.
     */
    public static void encounteredWildcardCertificate() {
        clientCertWildcardMetrics.inc();
    }

    /**
     * Reset the TLS metrics (for testing purposes)
     */
    public static void resetTlsMetrics() {
        clientCertSelfSignedMetrics.clear();
        clientCertSmallRsaKeySizeMetrics.clear();
        clientCertReachedWarnThresholdMetrics.clear();
        clientCertReachedErrorThresholdMetrics.clear();
        clientCertValidityDurationExceedsThresholdMetrics.clear();
        clientCertWildcardMetrics.clear();
    }

    public static double getClientCertSelfSignedCount() {
        return clientCertSelfSignedMetrics.get();
    }

    public static double getSmallRsaKeySizeCount() {
        return clientCertSmallRsaKeySizeMetrics.get();
    }

    public static double getCertificateUnderWarnThresholdCount() {
        return clientCertReachedWarnThresholdMetrics.get();
    }

    public static double getCertificateUnderErrorThresholdCount() {
        return clientCertReachedErrorThresholdMetrics.get();
    }

    public static double getLongValidityCertificateCount() {
        return clientCertValidityDurationExceedsThresholdMetrics.get();
    }

    public static double getWildcardCertificateCount(){
        return clientCertWildcardMetrics.get();
    }

}
