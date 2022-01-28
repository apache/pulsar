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
package org.apache.pulsar.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.InputStream;
import java.io.Serializable;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Map;
import java.util.Set;
import javax.naming.AuthenticationException;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Interface for accessing data which are used in variety of authentication schemes on client side.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface AuthenticationDataProvider extends Serializable {
    /*
     * TLS
     */

    /**
     * Check if data for TLS are available.
     *
     * @return true if this authentication data contain data for TLS
     */
    default boolean hasDataForTls() {
        return false;
    }

    /**
     *
     * @return a client certificate chain, or null if the data are not available
     */
    default Certificate[] getTlsCertificates() {
        return null;
    }

    /**
     * @return a client certificate file path
     */
    default String getTlsCerificateFilePath() {
        return null;
    }

    /**
     *
     * @return a private key for the client certificate, or null if the data are not available
     */
    default PrivateKey getTlsPrivateKey() {
        return null;
    }

    /**
     *
     * @return a private key file path
     */
    default String getTlsPrivateKeyFilePath() {
        return null;
    }

    /**
     *
     * @return an input-stream of the trust store, or null if the trust-store provided at
     *         {@link ClientConfigurationData#getTlsTrustStorePath()}
     */
    default InputStream getTlsTrustStoreStream() {
        return null;
    }

    /**
     * Used for TLS authentication with keystore type.
     *
     * @return a KeyStoreParams for the client certificate chain, or null if the data are not available
     */
    default KeyStoreParams getTlsKeyStoreParams() {
        return null;
    }

    /*
     * HTTP
     */

    /**
     * Check if data for HTTP are available.
     *
     * @return true if this authentication data contain data for HTTP
     */
    default boolean hasDataForHttp() {
        return false;
    }

    /**
     *
     * @return a authentication scheme, or {@code null} if the request will not be authenticated.
     */
    default String getHttpAuthType() {
        return null;
    }

    /**
     *
     * @return an enumeration of all the header names
     */
    default Set<Map.Entry<String, String>> getHttpHeaders() throws Exception {
        return null;
    }

    /*
     * Command
     */

    /**
     * Check if data from Pulsar protocol are available.
     *
     * @return true if this authentication data contain data from Pulsar protocol
     */
    default boolean hasDataFromCommand() {
        return false;
    }

    /**
     *
     * @return authentication data which will be stored in a command
     */
    default String getCommandData() {
        return null;
    }

    /**
     * For mutual authentication, This method use passed in `data` to evaluate and challenge,
     * then returns null if authentication has completed;
     * returns authenticated data back to server side, if authentication has not completed.
     *
     * <p>Mainly used for mutual authentication like sasl.
     */
    default AuthData authenticate(AuthData data) throws AuthenticationException {
        byte[] bytes = (hasDataFromCommand() ? this.getCommandData() : "").getBytes(UTF_8);
        return AuthData.of(bytes);
    }
}
