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
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;

public class AuthenticationProviderTls implements AuthenticationProvider {

    private enum ErrorCode {
        UNKNOWN,
        INVALID_CERTS,
        INVALID_CN, // invalid common name
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
        return "tls";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        String commonName = null;
        ErrorCode errorCode = ErrorCode.UNKNOWN;
        try {
            if (authData.hasDataFromTls()) {
                /**
                 * Maybe authentication type should be checked if it is an HTTPS session. However this check fails
                 * actually because authType is null.
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
                Certificate[] certs = authData.getTlsCertificates();
                if (null == certs) {
                    errorCode = ErrorCode.INVALID_CERTS;
                    throw new AuthenticationException("Failed to get TLS certificates from client");
                }
                String distinguishedName = ((X509Certificate) certs[0]).getSubjectX500Principal().getName();
                for (String keyValueStr : distinguishedName.split(",")) {
                    String[] keyValue = keyValueStr.split("=", 2);
                    if (keyValue.length == 2 && "CN".equals(keyValue[0]) && !keyValue[1].isEmpty()) {
                        commonName = keyValue[1];
                        break;
                    }
                }
            }

            if (commonName == null) {
                errorCode = ErrorCode.INVALID_CN;
                throw new AuthenticationException("Client unable to authenticate with TLS certificate");
            }
            AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
        } catch (AuthenticationException exception) {
            incrementFailureMetric(errorCode);
            throw exception;
        }
        return commonName;
    }

}
