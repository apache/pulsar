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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for the abortTransaction admin API with authentication and authorization enabled.
 */
@Test(groups = "broker-admin")
public class AdminApiTransactionWithAuthTest extends MockedPulsarServiceBaseTest {
    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    private static final String ADMIN = "admin";
    private static final String PROXY = "proxy";
    private static final String TXN_OWNER = "txnOwner";
    private static final String OTHER = "other";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setTransactionCoordinatorEnabled(true);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
        conf.setAuthenticationProviders(Set.of(AuthenticationProviderToken.class.getName()));
        conf.setSuperUserRoles(Set.of(ADMIN));
        conf.setProxyRoles(Set.of(PROXY));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + token(ADMIN));
        super.internalSetup();

        closeAdmin();
        admin = newAdmin(ADMIN);
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1"), Set.of("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Set.of("test"));
        // lets the transaction owner look up the transaction coordinator
        admin.namespaces().grantPermissionOnNamespace(NamespaceName.SYSTEM_NAMESPACE.toString(), TXN_OWNER,
                Set.of(AuthAction.consume));

        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(1));
        admin.lookups().lookupTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());
        // connecting a transaction-enabled client loads the transaction coordinator
        replacePulsarClient(PulsarClient.builder().serviceUrl(lookupUrl.toString()).enableTransaction(true)
                .authentication(AuthenticationToken.class.getName(), token(TXN_OWNER)));
        Awaitility.await().until(() -> pulsar.getTransactionMetadataStoreService().getStores().size() == 1);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static String token(String subject) {
        return Jwts.builder().setSubject(subject).signWith(SECRET_KEY).compact();
    }

    private PulsarAdmin newAdmin(String role) throws Exception {
        return PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(AuthenticationToken.class.getName(), token(role)).build();
    }

    private TxnID newOwnedTransaction() throws Exception {
        TxnID txnID = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES).build().get()
                .getTxnID();
        TxnMeta txnMeta = pulsar.getTransactionMetadataStoreService().getTxnMeta(txnID).get();
        assertEquals(txnMeta.status(), TxnStatus.OPEN);
        assertEquals(txnMeta.getOwner(), TXN_OWNER);
        return txnID;
    }

    private void abort(String role, TxnID txnID) throws Exception {
        @Cleanup
        PulsarAdmin roleAdmin = newAdmin(role);
        roleAdmin.transactions().abortTransaction(txnID);
    }

    private int abortWithOriginalPrincipal(String role, String originalPrincipal, TxnID txnID) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(brokerUrl + "/admin/v3/transactions/abortTransaction/"
                        + txnID.getMostSigBits() + "/" + txnID.getLeastSigBits()))
                .header("Authorization", "Bearer " + token(role))
                .header("X-Original-Principal", originalPrincipal)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private void assertAborted(TxnID txnID) {
        ExecutionException e = expectThrows(ExecutionException.class,
                () -> pulsar.getTransactionMetadataStoreService().getTxnMeta(txnID).get());
        assertTrue(e.getCause() instanceof CoordinatorException.TransactionNotFoundException);
    }

    private void assertOpen(TxnID txnID) throws Exception {
        assertEquals(pulsar.getTransactionMetadataStoreService().getTxnMeta(txnID).get().status(), TxnStatus.OPEN);
    }

    @Test
    public void testOwnerCanAbort() throws Exception {
        TxnID txnID = newOwnedTransaction();
        abort(TXN_OWNER, txnID);
        assertAborted(txnID);
    }

    @Test
    public void testSuperUserCanAbort() throws Exception {
        TxnID txnID = newOwnedTransaction();
        abort(ADMIN, txnID);
        assertAborted(txnID);
    }

    @Test
    public void testOtherRoleCannotAbort() throws Exception {
        TxnID txnID = newOwnedTransaction();
        assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> abort(OTHER, txnID));
        assertOpen(txnID);
    }

    @Test
    public void testOwnerCanAbortThroughProxy() throws Exception {
        TxnID txnID = newOwnedTransaction();
        assertEquals(abortWithOriginalPrincipal(PROXY, OTHER, txnID), 401);
        assertOpen(txnID);
        assertEquals(abortWithOriginalPrincipal(PROXY, TXN_OWNER, txnID), 204);
        assertAborted(txnID);
    }

    @Test
    public void testOriginalPrincipalIgnoredWithoutProxyRole() throws Exception {
        TxnID txnID = newOwnedTransaction();
        assertEquals(abortWithOriginalPrincipal(OTHER, TXN_OWNER, txnID), 401);
        assertOpen(txnID);
    }

    @Test
    public void testTransactionWithoutOwnerRequiresSuperUser() throws Exception {
        TxnID txnID = pulsar.getTransactionMetadataStoreService()
                .newTransaction(TransactionCoordinatorID.get(0), TimeUnit.MINUTES.toMillis(5), null).get();
        assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> abort(OTHER, txnID));
        assertOpen(txnID);
        abort(ADMIN, txnID);
        assertAborted(txnID);
    }

    @Test
    public void testMissingTransactionRequiresSuperUser() throws Exception {
        TxnID txnID = new TxnID(0, Long.MAX_VALUE);
        assertThrows(PulsarAdminException.NotAuthorizedException.class, () -> abort(OTHER, txnID));
    }
}
