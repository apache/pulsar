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
package org.apache.pulsar.broker.authorization;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.var;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MultiRolesTokenAuthorizationProviderTest {

    public static CompletableFuture<Boolean> testAuthz(String role){
        if(role.equals("b"))
            return CompletableFuture.completedFuture(true);
        return CompletableFuture.completedFuture(false);
    }

    public static CompletableFuture<Boolean> testAuthz2(String role){
        return CompletableFuture.completedFuture(false);
    }

    @Test
    public void test(){
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String token = Jwts.builder().claim("sub", new String[]{"a", "b"}).signWith(secretKey)
        .compact();
    }

    @Test
    public void testMultiRolesAuthz() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        String userA = "user-a";
        String userB = "user-b";
        String token = Jwts.builder().claim("sub", new String[]{userA, userB}).signWith(secretKey).compact();

        MultiRolesTokenAuthorizationProvider provider = new MultiRolesTokenAuthorizationProvider();

        AuthenticationDataSource ads = new AuthenticationDataSource() {
            @Override
            public boolean hasDataFromHttp() {
                return true;
            }

            @Override
            public String getHttpHeader(String name) {
                if (name.equals("Authorization")) {
                    return "Bearer " + token;
                } else {
                    throw new IllegalArgumentException("Wrong HTTP header");
                }
            }
        };

        Assert.assertTrue(provider.authorize(ads, role -> {
            if(role.equals(userB))return CompletableFuture.completedFuture(true); // only userB has permission
            return CompletableFuture.completedFuture(false);
        }).get());

        Assert.assertTrue(provider.authorize(ads, role -> {
            return CompletableFuture.completedFuture(true); // all users has permission
        }).get());

        Assert.assertFalse(provider.authorize(ads, role -> {
            return CompletableFuture.completedFuture(false); // only users has no permission
        }).get());
    }
}
