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
package org.apache.pulsar.functions.auth;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;

public class ClearTextFunctionTokenAuthProviderTest {

    @Test
    public void testClearTextAuth() throws Exception {

        ClearTextFunctionTokenAuthProvider clearTextFunctionTokenAuthProvider = new ClearTextFunctionTokenAuthProvider();
        Function.FunctionDetails funcDetails = Function.FunctionDetails.newBuilder().setTenant("test-tenant").setNamespace("test-ns").setName("test-func").build();

        Optional<FunctionAuthData> functionAuthData = clearTextFunctionTokenAuthProvider.cacheAuthData(funcDetails, new AuthenticationDataSource() {
                    @Override
                    public boolean hasDataFromCommand() {
                        return true;
                    }

                    @Override
                    public String getCommandData() {
                        return "test-token";
                    }
        });

        Assert.assertTrue(functionAuthData.isPresent());
        Assert.assertEquals(functionAuthData.get().getData(), "test-token".getBytes());

        AuthenticationConfig authenticationConfig = AuthenticationConfig.builder().build();
        clearTextFunctionTokenAuthProvider.configureAuthenticationConfig(authenticationConfig, functionAuthData);

        Assert.assertEquals(authenticationConfig.getClientAuthenticationPlugin(), AuthenticationToken.class.getName());
        Assert.assertEquals(authenticationConfig.getClientAuthenticationParameters(), "token:test-token");


        AuthenticationToken authenticationToken = new AuthenticationToken();
        authenticationToken.configure(authenticationConfig.getClientAuthenticationParameters());
        Assert.assertEquals(authenticationToken.getAuthData().getCommandData(), "test-token");
    }
}
