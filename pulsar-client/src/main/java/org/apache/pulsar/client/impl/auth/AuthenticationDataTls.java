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
package org.apache.pulsar.client.impl.auth;

import java.security.KeyManagementException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.util.SecurityUtility;

import lombok.Getter;

public class AuthenticationDataTls implements AuthenticationDataProvider {

    @Getter
    protected final X509Certificate[] tlsCertificates;
    @Getter
    protected final PrivateKey tlsPrivateKey;
    @Getter
    private String certFilePath, keyFilePath;
    public AuthenticationDataTls(String certFilePath, String keyFilePath) throws KeyManagementException {
        if (certFilePath == null) {
            throw new IllegalArgumentException("certFilePath must not be null");
        }
        if (keyFilePath == null) {
            throw new IllegalArgumentException("keyFilePath must not be null");
        }
        this.certFilePath = certFilePath;
        this.keyFilePath = keyFilePath;
        this.tlsCertificates = SecurityUtility.loadCertificatesFromPemFile(certFilePath);
        this.tlsPrivateKey = SecurityUtility.loadPrivateKeyFromPemFile(keyFilePath);
    }

    /*
     * TLS
     */

    @Override
    public boolean hasDataForTls() {
        return true;
    }

}
