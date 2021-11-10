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
package org.apache.pulsar.broker.authentication;

import java.io.IOException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import javax.naming.AuthenticationException;
import javax.security.auth.x500.X500Principal;

import lombok.NonNull;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationProviderTls implements AuthenticationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationProviderTls.class);

    boolean logEntireCertificateChain;
    boolean printWarnOnSelfSignedCertificate;
    int printWarnIfRsaKeySizeLessThanBits;
    long printWarnOnClientCertNearingExpirationMillis;
    long printErrorOnClientCertNearingExpirationMillis;
    long printWarnOnClientCertValidityDurationExceedsMillis;
    boolean printWarnOnWildcardCertificate;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        this.logEntireCertificateChain = config.isTlsLogEntireCertificateChain();

        this.printWarnOnSelfSignedCertificate = config.isTlsPrintWarnOnSelfSignedCertificate();
        if(this.printWarnOnSelfSignedCertificate) {
            LOG.info("Broker will emit warnings when a self-signed client cert is encountered");
        }

        if(0 < config.getTlsPrintWarnOnRsaKeySizeLessThanBits()) {
            LOG.info("Broker will emit warnings when a certificate has RSA key size less than "
                    + this.printWarnIfRsaKeySizeLessThanBits + " bits");
            this.printWarnIfRsaKeySizeLessThanBits = config.getTlsPrintWarnOnRsaKeySizeLessThanBits();
        } else if (0 > config.getTlsPrintWarnOnRsaKeySizeLessThanBits()) {
            LOG.warn("Supplied tlsPrintWarnOnRsaKeySizeLessThanBits is negative which is not a valid value");
            this.printWarnIfRsaKeySizeLessThanBits = 0;
        }

        if(0 < config.getTlsPrintWarnOnClientCertNearingExpirationMillis()) {
            LOG.info("Broker will emit warnings when a certificate expiring within "
                    + this.printWarnOnClientCertNearingExpirationMillis + " milliseconds is encountered");
            this.printWarnOnClientCertNearingExpirationMillis
                    = config.getTlsPrintWarnOnClientCertNearingExpirationMillis();
        } else if (0 > config.getTlsPrintWarnOnClientCertNearingExpirationMillis()) {
            LOG.warn("Supplied tlsPrintWarnOnClientCertNearingExpirationMillis is negative which is not a valid value");
            this.printWarnOnClientCertNearingExpirationMillis = 0;
        }

        if(0 < config.getTlsPrintErrorOnClientCertNearingExpirationMillis()) {
            LOG.info("Broker will emit errors when a certificate expiring within "
                    + this.printErrorOnClientCertNearingExpirationMillis + " milliseconds is encountered");
            this.printErrorOnClientCertNearingExpirationMillis
                    = config.getTlsPrintErrorOnClientCertNearingExpirationMillis();
        } else if (0 > config.getTlsPrintErrorOnClientCertNearingExpirationMillis()) {
            LOG.warn("Supplied tlsPrintErrorOnClientCertNearingExpirationMillis is negative which is not a valid value");
            this.printErrorOnClientCertNearingExpirationMillis = 0;
        }

        if(0 < config.getTlsPrintWarnOnClientCertValidityDurationExceedsMillis()) {
            LOG.info("Broker will emit warnings when encountering a certificate with validity period (notAfter - notBefore) exceeding "
                    + this.printWarnOnClientCertValidityDurationExceedsMillis + " milliseconds");
            this.printWarnOnClientCertValidityDurationExceedsMillis
                    = config.getTlsPrintWarnOnClientCertValidityDurationExceedsMillis();
        } else if (0 > config.getTlsPrintWarnOnClientCertValidityDurationExceedsMillis()) {
            LOG.warn("Supplied tsPrintWarnOnClientCertValidityDurationExceedsMillis is negative which is not a valid value");
            this.printWarnOnClientCertValidityDurationExceedsMillis = 0;
        }

        this.printWarnOnWildcardCertificate = config.isTlsPrintWarnOnWildcardCertificate();
        if(this.printWarnOnWildcardCertificate) {
            LOG.info("Broker will emit warnings when encountering a wildcard certificate");
        }
    }

    @Override
    public String getAuthMethodName() {
        return "tls";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        String commonName = null;
        X509Certificate[] clientCertificateChain = null;
        try {
            if (authData.hasDataFromTls()) {
                /**
                 * Maybe authentication type should be checked if it is an HTTPS session. However this check fails actually
                 * because authType is null.
                 *
                 * This check is not necessarily needed, because an untrusted certificate is not passed to
                 * HttpServletRequest.
                 *
                 * <code>
                 * if (authData.hasDataFromHttp()) {
                 *     String authType = authData.getHttpAuthType();
                 *     if (!HttpServletRequest.CLIENT_CERT_AUTH.equals(authType)) {
                 *         throw new AuthenticationException(
                 *              String.format( "Authentication type mismatch, Expected: %s, Found: %s",
                 *                       HttpServletRequest.CLIENT_CERT_AUTH, authType));
                 *     }
                 * }
                 * </code>
                 */

                // Extract CommonName
                // The format is defined in RFC 2253.
                // Example:
                // CN=Steve Kille,O=Isode Limited,C=GB
                Certificate[] uncastCerts = authData.getTlsCertificates();
                clientCertificateChain = new X509Certificate[uncastCerts.length];

                for(int i = 0; i < uncastCerts.length; i++) {
                    clientCertificateChain[i] = (X509Certificate) uncastCerts[i];
                }

                if (null == clientCertificateChain) {
                    throw new AuthenticationException("Failed to get TLS certificates from client");
                }
                String distinguishedName = clientCertificateChain[0].getSubjectX500Principal().getName();
                for (String keyValueStr : distinguishedName.split(",")) {
                    String[] keyValue = keyValueStr.split("=", 2);
                    if (keyValue.length == 2 && "CN".equals(keyValue[0]) && !keyValue[1].isEmpty()) {
                        commonName = keyValue[1];
                        break;
                    }
                }


            }

            if (commonName == null) {
                throw new AuthenticationException("Client unable to authenticate with TLS certificate");
            }

            if(this.printWarnOnSelfSignedCertificate) {
                checkIfSelfSignedCertificate(clientCertificateChain[0], commonName);
            }

            if(0 != this.printWarnIfRsaKeySizeLessThanBits) {
                checkIfUsingSmallRsaKeySize(clientCertificateChain[0], this.printWarnIfRsaKeySizeLessThanBits,
                        commonName);
            }

            if(this.logEntireCertificateChain) {
                LOG.info("Authenticated client cert with chain: " + concatenateFullCertChain(clientCertificateChain));
            }

            if(0 != this.printWarnOnClientCertNearingExpirationMillis ||
                    0 != this.printErrorOnClientCertNearingExpirationMillis) {
                checkIfNearingExpiration(clientCertificateChain[0],
                        commonName,
                        this.printErrorOnClientCertNearingExpirationMillis,
                        this.printWarnOnClientCertNearingExpirationMillis);
            }

            if(0 != this.printWarnOnClientCertValidityDurationExceedsMillis) {
                checkMaxValidityPeriod(clientCertificateChain[0],
                        commonName,
                        this.printWarnOnClientCertValidityDurationExceedsMillis);
            }

            if(this.printWarnOnWildcardCertificate) {
                checkIfWildcardCertificate(commonName);
            }

            AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
        } catch (AuthenticationException exception) {
            AuthenticationMetrics.authenticateFailure(getClass().getSimpleName(), getAuthMethodName(), exception.getMessage());
            throw exception;
        }
        return commonName;
    }

    /**
     * According to RFC3280 "A certificate is self-issued if the DNs that appear in the subject and issuer
     * fields are identical and are not empty." Extracting the X500Principal object representing the subject
     * and the issuer and comparing using .equals(...) is sufficient for confirming this.
     *
     *  It is possible to create a certificate with an empty DN but this method expects that the DN has at
     *  least a CN (common name) attribute.
     *
     * @param x509Certificate certificate to check
     * @param commonName common name extracted from the certificate
     */
    static void checkIfSelfSignedCertificate(@NonNull final X509Certificate x509Certificate,
                                             @NonNull final String commonName) {
        X500Principal subject = x509Certificate.getSubjectX500Principal();
        X500Principal issuer = x509Certificate.getIssuerX500Principal();

        if(subject.equals(issuer)) {
            LOG.warn("Encountered self-signed certificate with CN " + commonName);
            AuthenticationMetrics.encounteredSelfSignedCertificate();
        }
    }

    /**
     * This method checks for the presence of a wildcard (*) in the common name of the certificate.
     * If a wildcard is found a log message is emitted and the Prometheus counter:
     * pulsar_authentication_tls_cert_wildcard_count is incremented
     *
     * @param commonName to check
     */
    static void checkIfWildcardCertificate(@NonNull final String commonName) {
        if(commonName.contains("*")) {
            LOG.warn("Encountered client certificate with CN " + commonName + " that contains a wildcard");
            AuthenticationMetrics.encounteredWildcardCertificate();
        }
    }

    /**
     * This checks whether the supplied certificate's public key bit length is less than the supplied
     * minimum allowable bit length. If the key is too small, this method will emit a warning and
     * increment the Prometheus counter: pulsar_authentication_tls_cert_small_rsa_key_count
     *
     * Note: This method DOES NOT work for certificates with DSA keys only RSA
     *
     * @param x509Certificate certificate to check
     * @param minimumAllowableBitLength smallest acceptable RSA key size
     * @param commonName extracted from the certificate (for logging)
     */
    static void checkIfUsingSmallRsaKeySize(@NonNull final X509Certificate x509Certificate,
                                            final int minimumAllowableBitLength,
                                            @NonNull final String commonName) {
        PublicKey publicKey = x509Certificate.getPublicKey();
        if(publicKey instanceof RSAPublicKey) {
            if(((RSAPublicKey) publicKey).getModulus().bitLength() < minimumAllowableBitLength) {
                LOG.warn("Encountered a client certificate with CN " + commonName + " and RSA key size < " +
                        minimumAllowableBitLength);
                AuthenticationMetrics.encounteredSmallRsaKeySize();
            }
        }
    }

    /**
     * The following method extracts the 'notAfter' attribute from the client's certificate
     * and compares it to the printWarnOnClientCertNearingExpirationMillis and
     * printErrorOnClientCertNearingExpirationMillis properties supplied as parameters.
     *
     * Those two parameters control the definition of 'nearing expiration' and
     * 'imminent expiration' and print warnings and errors accordingly. These are relative terms
     * and highly dependent on the environment's particular PKI setup. If the values are set too high
     * the broker logs will be spammed with unnecessary messages. Even worse, if the values are
     * set too low then the messages will be emitted but the admin may not have enough time to catch
     * the problem and contact the user to inform them they need to rotate/re-issue the offending cert.
     *
     * In addition to printing warnings/errors this method will increment the metrics:
     * pulsar_authentication_tls_cert_expiration_soon_count and
     * pulsar_authentication_tls_cert_expiration_imminent_count
     *
     * @param x509Certificate to check
     * @param commonName previously extracted from cert (used for logging)
     * @param printErrorOnClientCertNearingExpirationMillis threshold for generating an error message
     * @param printWarnOnClientCertNearingExpirationMillis threshold for generating a warning message
     */
    static void checkIfNearingExpiration(X509Certificate x509Certificate,
                                         final String commonName,
                                         final long printErrorOnClientCertNearingExpirationMillis,
                                         final long printWarnOnClientCertNearingExpirationMillis) {
        Instant notAfter = x509Certificate.getNotAfter().toInstant();
        Instant now = Instant.now();
        Instant errorInstant = notAfter.minusMillis(printErrorOnClientCertNearingExpirationMillis);
        Instant warnInstant = notAfter.minusMillis(printWarnOnClientCertNearingExpirationMillis);


        if(warnInstant.isBefore(now)) {
            LOG.warn("Client certificate with CN " + commonName + " is nearing expiration (" + notAfter + ")");
            AuthenticationMetrics.encounteredCertificateUnderWarnThreshold();
        } else if(errorInstant.isBefore(now)) {
            LOG.error("Expiration of client certificate with CN " + commonName + " is imminent (" + notAfter + ")");
            AuthenticationMetrics.encounteredCertificateUnderErrorThreshold();
        }
    }

    /**
     * This method checks if the supplied certificate has a validity period exceeding the supplied
     * number of milliseconds. If the period exceeds the limit then an WARN message is printed in
     * the application logs and the metric: pulsar_authentication_tls_cert_validity_too_long_count
     * is incremented. If the period does not exceed the limit and DEBUG is ON, then the validity
     * period is printed in the logs for debugging purposes.
     *
     * @param x509Certificate to check
     * @param commonName previously extracted from cert (used for logging)
     * @param printWarnOnClientCertValidityDurationExceedsMillis maximum valid duration in milliseconds
     */
    static void checkMaxValidityPeriod(@NonNull X509Certificate x509Certificate,
                                       final String commonName,
                                       final long printWarnOnClientCertValidityDurationExceedsMillis) {
        Instant notAfter = x509Certificate.getNotAfter().toInstant();
        Instant notBefore = x509Certificate.getNotBefore().toInstant();

        Duration maximumValidityDuration = Duration.ofMillis(printWarnOnClientCertValidityDurationExceedsMillis);

        Duration validityPeriod = Duration.ofMillis(ChronoUnit.MILLIS.between(notBefore, notAfter));

        if(Duration.from(validityPeriod).toMillis() > maximumValidityDuration.toMillis() ) {
            LOG.warn("Encountered certificate with CN " + commonName + " and validity period exceeding "
                    + maximumValidityDuration + " (" + validityPeriod + ")");
            AuthenticationMetrics.encounteredLongValidityCertificate();
        } else if(LOG.isDebugEnabled()) {
            LOG.debug("Encountered certificate with CN " + commonName + " and validity period " + validityPeriod);
        }
    }

    /**
     * This will return the entire validated cert chain starting with root -> intermediate ->
     * ... -> client cert
     *
     * Useful if you need to debug/inspect the complete trust chain or are migrating from one
     * cert provider to another.
     *
     * @param certs chain to concatenate
     * @return the distinguished names of the cert chain in order from root to client
     */
    static String concatenateFullCertChain(final Certificate[] certs) {
        final Certificate[] reversedArray = Arrays.copyOf(certs, certs.length);
        Collections.reverse(Arrays.asList(reversedArray));

        return Arrays.stream(reversedArray)
                .map(c -> (X509Certificate) c)
                .map(X509Certificate::getSubjectX500Principal)
                .map(X500Principal::getName)
                .collect(Collectors.joining(" -> "));
    }

}
