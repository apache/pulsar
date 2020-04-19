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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.util.FileModifiedTimeUpdater;
import org.apache.pulsar.common.util.SecurityUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationDataTls implements AuthenticationDataProvider {
    protected X509Certificate[] tlsCertificates;
    protected PrivateKey tlsPrivateKey;
    private FileModifiedTimeUpdater certFile, keyFile;
    // key and cert using stream
    private InputStream certStream, keyStream;
    private Supplier<ByteArrayInputStream> certStreamProvider, keyStreamProvider;

    public AuthenticationDataTls(String certFilePath, String keyFilePath) throws KeyManagementException {
        if (certFilePath == null) {
            throw new IllegalArgumentException("certFilePath must not be null");
        }
        if (keyFilePath == null) {
            throw new IllegalArgumentException("keyFilePath must not be null");
        }
        this.certFile = new FileModifiedTimeUpdater(certFilePath);
        this.keyFile = new FileModifiedTimeUpdater(keyFilePath);
        this.tlsCertificates = SecurityUtility.loadCertificatesFromPemFile(certFilePath);
        this.tlsPrivateKey = SecurityUtility.loadPrivateKeyFromPemFile(keyFilePath);
    }

    public AuthenticationDataTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider) throws KeyManagementException {
        if (certStreamProvider == null || certStreamProvider.get() == null) {
            throw new IllegalArgumentException("certStream provider or stream must not be null");
        }
        if (keyStreamProvider == null || keyStreamProvider.get() == null) {
            throw new IllegalArgumentException("keyStream provider or stream must not be null");
        }
        this.certStreamProvider = certStreamProvider;
        this.keyStreamProvider = keyStreamProvider;
        this.certStream = certStreamProvider.get();
        this.keyStream = keyStreamProvider.get();
        this.tlsCertificates = SecurityUtility.loadCertificatesFromPemStream(certStream);
        this.tlsPrivateKey = SecurityUtility.loadPrivateKeyFromPemStream(keyStream);
    }

    /*
     * TLS
     */

    @Override
    public boolean hasDataForTls() {
        return true;
    }

    @Override
    public Certificate[] getTlsCertificates() {
        if (certFile != null && certFile.checkAndRefresh()) {
            try {
                this.tlsCertificates = SecurityUtility.loadCertificatesFromPemFile(certFile.getFileName());
            } catch (KeyManagementException e) {
                LOG.error("Unable to refresh authData for cert {}: ", certFile.getFileName(), e);
            }
        } else if (certStreamProvider != null && certStreamProvider.get() != null
                && !certStreamProvider.get().equals(certStream)) {
            try {
                certStream = certStreamProvider.get();
                tlsCertificates = SecurityUtility.loadCertificatesFromPemStream(certStream);
            } catch (KeyManagementException e) {
                LOG.error("Unable to refresh authData from cert stream ", e);
            }
        }
        return this.tlsCertificates;
    }

    @Override
    public PrivateKey getTlsPrivateKey() {
        if (keyFile != null && keyFile.checkAndRefresh()) {
            try {
                this.tlsPrivateKey = SecurityUtility.loadPrivateKeyFromPemFile(keyFile.getFileName());
            } catch (KeyManagementException e) {
                LOG.error("Unable to refresh authData for cert {}: ", keyFile.getFileName(), e);
            }
        } else if (keyStreamProvider != null && keyStreamProvider.get() != null
                && !keyStreamProvider.get().equals(keyStream)) {
            try {
                keyStream = keyStreamProvider.get();
                tlsPrivateKey = SecurityUtility.loadPrivateKeyFromPemStream(keyStream);
            } catch (KeyManagementException e) {
                LOG.error("Unable to refresh authData from key stream ", e);
            }
        }
        return this.tlsPrivateKey;
    }

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationDataTls.class);
}
