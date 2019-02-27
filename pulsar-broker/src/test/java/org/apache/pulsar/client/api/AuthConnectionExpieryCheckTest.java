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

import static org.mockito.Mockito.spy;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.ClientAuthentication;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthenticationProvider;
import org.apache.pulsar.client.api.AuthorizationProducerConsumerTest.TestAuthorizationProvider;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class AuthConnectionExpieryCheckTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(AuthConnectionExpieryCheckTest.class);

    private final static String clientRole = "plugbleRole";
    private static int keepAliveInSec = 2;
    private static int cnxExpriyTime = keepAliveInSec * 2;
    private static boolean isExpired = false;
    private static boolean init = false;

    protected void setup() throws Exception {

        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("superUser");
        conf.setSuperUserRoles(superUserRoles);

        conf.setClusterName("test");

        super.init();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "expired")
    public Object[][] expiredrovider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Test(dataProvider = "expired")
    public void testCnxExpiryCheck(boolean expired) throws Exception {
        log.info("-- Starting {} test --", methodName);

        isExpired = expired;
        init = false;
        conf.setKeepAliveIntervalSeconds(keepAliveInSec);
        conf.setAuthenticationProviders(ImmutableSet.of(MockAuthenticationProvider.class.getName()));
        conf.setAuthorizationProvider(TestAuthorizationProvider.class.getName());
        conf.setAuthenticationCheckExpiredConnectionEnabled(true);
        setup();

        Authentication adminAuthentication = new ClientAuthentication("superUser");
        admin = spy(
                PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).authentication(adminAuthentication).build());

        String lookupUrl = "pulsar://localhost:" + BROKER_PORT;
        lookupUrl = new URI(lookupUrl).toString();

        Authentication authentication = new ClientAuthentication(clientRole);

        pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl).authentication(authentication).build();

        ClusterData clusterData = new ClusterData(lookupUrl);

        admin.clusters().createCluster("test", clusterData);
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic").subscriptionName("my-subscriber-name").subscribe();
        ClientCnx cnxBeforeExpriy = consumer.getClientCnx();
        // sleep until cert expires and renew certs handshake completes
        Thread.sleep(TimeUnit.SECONDS.toMillis((cnxExpriyTime) + (2 * keepAliveInSec)));
        retryStrategically((test) -> isExpired == (cnxBeforeExpriy != consumer.getClientCnx()), 5, 500);
        ClientCnx cnxAfterExpriy = consumer.getClientCnx();
        // if cnx-gets expires then broker should close the cnx and new cnx must be created
        Assert.assertEquals(isExpired, cnxBeforeExpriy != cnxAfterExpriy);

        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    public static class MockAuthenticationProvider extends TestAuthenticationProvider {

        @Override
        public long getExpiryTime(AuthenticationDataSource authData) {
            long addExpireyBuffer = TimeUnit.SECONDS.toMillis(cnxExpriyTime);
            // return expired after first successful connection creation
            long expireMillis = (System.currentTimeMillis() + (addExpireyBuffer * (init && isExpired ? -1 : 1)));
            if (!init && !isExpired) {
                // if renewed cert then return expiry longer to avoid any testing conflict
                expireMillis *= expireMillis;
            }
            init = true;
            return expireMillis;
        }
    }

}
