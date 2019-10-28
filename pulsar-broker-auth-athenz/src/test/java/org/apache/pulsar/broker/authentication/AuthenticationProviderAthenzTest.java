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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.yahoo.athenz.auth.token.RoleToken;
import com.yahoo.athenz.zpe.ZpeConsts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AuthenticationProviderAthenzTest {

    ServiceConfiguration config;
    Properties properties;
    AuthenticationProviderAthenz provider;

    @BeforeClass
    public void setup() throws Exception {

        // Set provider domain name
        properties = new Properties();
        properties.setProperty("athenzDomainNames", "test_provider");
        config = new ServiceConfiguration();
        config.setProperties(properties);

        // Initialize authentication provider
        provider = new AuthenticationProviderAthenz();
        provider.initialize(config);

        // Specify Athenz configuration file for AuthZpeClient which is used in AuthenticationProviderAthenz
        System.setProperty(ZpeConsts.ZPE_PROP_ATHENZ_CONF, "./src/test/resources/athenz.conf.test");
    }

    @Test
    public void testInitilizeFromSystemPropeties() {
        System.setProperty("pulsar.athenz.domain.names", "test_provider");
        ServiceConfiguration emptyConf = new ServiceConfiguration();
        Properties emptyProp = new Properties();
        emptyConf.setProperties(emptyProp);
        AuthenticationProviderAthenz sysPropProvider1 = new AuthenticationProviderAthenz();
        try {
            sysPropProvider1.initialize(emptyConf);
            assertEquals(sysPropProvider1.getAllowedOffset(), 30); // default allowed offset is 30 sec
        } catch (Exception e) {
            fail("Fail to Read pulsar.athenz.domain.names from System Properties");
        }

        System.setProperty("pulsar.athenz.role.token_allowed_offset", "0");
        AuthenticationProviderAthenz sysPropProvider2 = new AuthenticationProviderAthenz();
        try {
            sysPropProvider2.initialize(config);
            assertEquals(sysPropProvider2.getAllowedOffset(), 0);
        } catch (Exception e) {
            fail("Failed to get allowd offset from system property");
        }

        System.setProperty("pulsar.athenz.role.token_allowed_offset", "invalid");
        AuthenticationProviderAthenz sysPropProvider3 = new AuthenticationProviderAthenz();
        try {
            sysPropProvider3.initialize(config);
            fail("Invalid allowed offset should not be specified");
        } catch (IOException e) {
        }

        System.setProperty("pulsar.athenz.role.token_allowed_offset", "-1");
        AuthenticationProviderAthenz sysPropProvider4 = new AuthenticationProviderAthenz();
        try {
            sysPropProvider4.initialize(config);
            fail("Negative allowed offset should not be specified");
        } catch (IOException e) {
        }
    }

    @Test
    public void testAuthenticateSignedToken() throws Exception {

        List<String> roles = new ArrayList<String>() {
            {
                add("test_role");
            }
        };
        RoleToken token = new RoleToken.Builder("Z1", "test_provider", roles).principal("test_app").build();
        String privateKey = new String(Files.readAllBytes(Paths.get("./src/test/resources/zts_private.pem")));
        token.sign(privateKey);
        AuthenticationDataSource authData = new AuthenticationDataCommand(token.getSignedToken(),
                new InetSocketAddress("localhost", 0), null);
        assertEquals(provider.authenticate(authData), "test_app");
    }

    @Test
    public void testAuthenticateUnsignedToken() throws Exception {

        List<String> roles = new ArrayList<String>() {
            {
                add("test_role");
            }
        };
        RoleToken token = new RoleToken.Builder("Z1", "test_provider", roles).principal("test_app").build();
        AuthenticationDataSource authData = new AuthenticationDataCommand(token.getUnsignedToken(),
                new InetSocketAddress("localhost", 0), null);
        try {
            provider.authenticate(authData);
            fail("Unsigned token should not be authenticated");
        } catch (AuthenticationException e) {
            // OK, expected
        }
    }

    @Test
    public void testAuthenticateSignedTokenWithDifferentDomain() throws Exception {

        List<String> roles = new ArrayList<String>() {
            {
                add("test_role");
            }
        };
        RoleToken token = new RoleToken.Builder("Z1", "invalid", roles).principal("test_app").build();
        String privateKey = new String(Files.readAllBytes(Paths.get("./src/test/resources/zts_private.pem")));
        token.sign(privateKey);
        AuthenticationDataSource authData = new AuthenticationDataCommand(token.getSignedToken(),
                new InetSocketAddress("localhost", 0), null);
        try {
            provider.authenticate(authData);
            fail("Token which has different domain should not be authenticated");
        } catch (AuthenticationException e) {
            // OK, expected
        }
    }
}
