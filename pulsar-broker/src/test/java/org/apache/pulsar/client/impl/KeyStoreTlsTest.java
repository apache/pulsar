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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.util.SecurityUtility.getProvider;
import java.security.Provider;
import java.util.Collections;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
import org.apache.pulsar.common.util.keystoretls.SSLContextValidatorEngine;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class KeyStoreTlsTest {

    protected final String BROKER_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
    protected final String BROKER_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.truststore.jks";
    protected final String BROKER_KEYSTORE_PW = "111111";
    protected final String BROKER_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.keystore.jks";
    protected final String CLIENT_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
    protected final String CLIENT_KEYSTORE_PW = "111111";
    protected final String CLIENT_TRUSTSTORE_PW = "111111";
    protected final String KEYSTORE_TYPE = "JKS";

    protected final String BROKER_TRUSTSTORE_FILE_NPD_PATH =
            "./src/test/resources/authentication/keystoretls/client.truststore.nopassword.jks";

    protected final String CLIENT_TRUSTSTORE_FILE_NPD_PATH =
            "./src/test/resources/authentication/keystoretls/broker.truststore.nopassword.jks";

    public static final Provider BC_PROVIDER = getProvider();

    @Test(timeOut = 300000)
    public void testValidate() throws Exception {
        KeyStoreSSLContext serverSSLContext = new KeyStoreSSLContext(KeyStoreSSLContext.Mode.SERVER,
                null,
                KEYSTORE_TYPE,
                BROKER_KEYSTORE_FILE_PATH,
                BROKER_KEYSTORE_PW,
                false,
                KEYSTORE_TYPE,
                BROKER_TRUSTSTORE_FILE_PATH,
                BROKER_TRUSTSTORE_PW,
                true,
                null,
                null);
        serverSSLContext.createSSLContext();

        KeyStoreSSLContext clientSSLContext = new KeyStoreSSLContext(KeyStoreSSLContext.Mode.CLIENT,
                null,
                KEYSTORE_TYPE,
                CLIENT_KEYSTORE_FILE_PATH,
                CLIENT_KEYSTORE_PW,
                false,
                KEYSTORE_TYPE,
                CLIENT_TRUSTSTORE_FILE_PATH,
                CLIENT_TRUSTSTORE_PW,
                false,
                null,
                // set client's protocol to TLSv1.2 since SSLContextValidatorEngine.validate doesn't handle TLSv1.3
                Collections.singleton("TLSv1.2"));
        clientSSLContext.createSSLContext();

        SSLContextValidatorEngine.validate(clientSSLContext::createSSLEngine, serverSSLContext::createSSLEngine);
    }

    @Test(timeOut = 300000)
    public void testValidateKeyStoreNoPwd() throws Exception {
        KeyStoreSSLContext serverSSLContext = new KeyStoreSSLContext(KeyStoreSSLContext.Mode.SERVER,
                null,
                KEYSTORE_TYPE,
                BROKER_KEYSTORE_FILE_PATH,
                BROKER_KEYSTORE_PW,
                false,
                KEYSTORE_TYPE,
                BROKER_TRUSTSTORE_FILE_NPD_PATH,
                null,
                true,
                null,
                null);
        serverSSLContext.createSSLContext();

        KeyStoreSSLContext clientSSLContext = new KeyStoreSSLContext(KeyStoreSSLContext.Mode.CLIENT,
                null,
                KEYSTORE_TYPE,
                CLIENT_KEYSTORE_FILE_PATH,
                CLIENT_KEYSTORE_PW,
                false,
                KEYSTORE_TYPE,
                CLIENT_TRUSTSTORE_FILE_NPD_PATH,
                null,
                false,
                null,
                // set client's protocol to TLSv1.2 since SSLContextValidatorEngine.validate doesn't handle TLSv1.3
                Collections.singleton("TLSv1.2"));
        clientSSLContext.createSSLContext();

        SSLContextValidatorEngine.validate(clientSSLContext::createSSLEngine, serverSSLContext::createSSLEngine);
    }
}
