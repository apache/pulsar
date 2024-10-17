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
package org.apache.pulsar.broker.admin.v3;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import groovy.util.logging.Slf4j;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for transaction admin APIs with authentication.
 */
@Slf4j
@Test(groups = "broker-admin")
public class AdminApiTransactionWithAuthTest extends MockedPulsarServiceBaseTest {
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String ADMIN_TOKEN = Jwts.builder().setSubject("admin").signWith(SECRET_KEY).compact();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setTransactionCoordinatorEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);
        conf.setAuthenticationProviders(providers);
        super.internalSetup();

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(brokerUrl != null
                        ? brokerUrl.toString() : brokerUrlTls.toString())
                .authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN);
        closeAdmin();
        admin = Mockito.spy(pulsarAdminBuilder.build());
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1"), Set.of("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Set.of("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Set.of("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @SneakyThrows
    private void grantTxnLookupToRole(String role) {
        admin.namespaces().grantPermissionOnNamespace(
                NamespaceName.SYSTEM_NAMESPACE.toString(),
                role,
                Sets.newHashSet(AuthAction.consume));
    }

    private void initTransaction(int coordinatorSize) throws Exception {
        pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(coordinatorSize));
        admin.lookups().lookupTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true)
                .authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN));
        pulsarClient.close();
        pulsarClient = null;
        Awaitility.await().until(() ->
                pulsar.getTransactionMetadataStoreService().getStores().size() == coordinatorSize);
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true)
                .authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN));
    }

    @Test
    public void testAbortTransaction() throws Exception {
        initTransaction(1);
        String txnOwner = "txnOwner";
        String transactionOwnerToken = Jwts.builder().setSubject(txnOwner).signWith(SECRET_KEY).compact();
        String other = "other";
        String otherToken = Jwts.builder().setSubject(other).signWith(SECRET_KEY).compact();
        grantTxnLookupToRole(txnOwner);

        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .authentication(AuthenticationToken.class.getName(), transactionOwnerToken).enableTransaction(true));

        // 1. Transaction owner can abort their own transaction.
        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        TxnMeta txnMeta = pulsar.getTransactionMetadataStoreService().getTxnMeta(transaction.getTxnID()).get();
        assertEquals(txnMeta.status(), TxnStatus.OPEN);
        assertEquals(txnMeta.getOwner(), txnOwner);
        String serviceHttpUrl = brokerUrl != null ? brokerUrl.toString() : brokerUrlTls.toString();
        closeAdmin();
        admin = Mockito.spy(PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl)
                .authentication(AuthenticationToken.class.getName(), transactionOwnerToken).build());
        admin.transactions().abortTransaction(transaction.getTxnID());
        try {
            pulsar.getTransactionMetadataStoreService().getTxnMeta(transaction.getTxnID()).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CoordinatorException.TransactionNotFoundException);
        }

        // 2. Super user can abort any transaction.
        transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        txnMeta = pulsar.getTransactionMetadataStoreService().getTxnMeta(transaction.getTxnID()).get();
        assertEquals(txnMeta.status(), TxnStatus.OPEN);
        assertEquals(txnMeta.getOwner(), txnOwner);
        closeAdmin();
        admin = Mockito.spy(PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl)
                .authentication(AuthenticationToken.class.getName(), ADMIN_TOKEN).build());
        admin.transactions().abortTransaction(transaction.getTxnID());
        try {
            pulsar.getTransactionMetadataStoreService().getTxnMeta(transaction.getTxnID()).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CoordinatorException.TransactionNotFoundException);
        }

        // 3. Non-super user and non-transaction owner cannot abort the transaction.
        transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get();
        txnMeta = pulsar.getTransactionMetadataStoreService().getTxnMeta(transaction.getTxnID()).get();
        assertEquals(txnMeta.status(), TxnStatus.OPEN);
        assertEquals(txnMeta.getOwner(), txnOwner);
        closeAdmin();
        admin = Mockito.spy(PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl)
                .authentication(AuthenticationToken.class.getName(), otherToken).build());
        try {
            admin.transactions().abortTransaction(transaction.getTxnID());
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof PulsarAdminException.NotAuthorizedException);
        }
    }
}
