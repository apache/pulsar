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
package org.apache.bookkeeper.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.apache.bookkeeper.auth.AuthCallbacks;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.ClientConnectionPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This test verifies the auditor bookie scenarios from the auth point-of-view.
 */
public class AuthAutoRecoveryTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory
        .getLogger(AuthAutoRecoveryTest.class);

    public static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";

    private static String clientSideRole;

    private static class AuditorClientAuthInterceptorFactory
        implements ClientAuthProvider.Factory {

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ClientConfiguration conf) {
            clientSideRole = conf.getClientRole();
        }

        @Override
        public ClientAuthProvider newProvider(ClientConnectionPeer addr,
            final AuthCallbacks.GenericCallback<Void> completeCb) {
            return new ClientAuthProvider() {
                public void init(AuthCallbacks.GenericCallback<AuthToken> cb) {
                    completeCb.operationComplete(BKException.Code.OK, null);
                }

                public void process(AuthToken m, AuthCallbacks.GenericCallback<AuthToken> cb) {
                }
            };
        }
    }

    protected ServerConfiguration newServerConfiguration() throws Exception {
        ServerConfiguration conf = super.newServerConfiguration();
        conf.setClientAuthProviderFactoryClass(AuditorClientAuthInterceptorFactory.class.getName());
        return conf;
    }

    public AuthAutoRecoveryTest() {
        super(6);
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * test the client role of the auditor
     */
    @Test
    public void testAuthClientRole() throws Exception {
        ServerConfiguration config = confByIndex(0);
        assertEquals(AuditorClientAuthInterceptorFactory.class.getName(), config.getClientAuthProviderFactoryClass());
        AutoRecoveryMain main = new AutoRecoveryMain(config);
        try {
            main.start();
            Thread.sleep(500);
            assertTrue("AuditorElector should be running",
                main.auditorElector.isRunning());
            assertTrue("Replication worker should be running",
                main.replicationWorker.isRunning());
        } finally {
            main.shutdown();
        }
        assertEquals(ClientConfiguration.CLIENT_ROLE_SYSTEM, clientSideRole);
    }

}
