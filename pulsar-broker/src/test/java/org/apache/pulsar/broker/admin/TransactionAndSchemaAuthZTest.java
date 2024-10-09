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
package org.apache.pulsar.broker.admin;

import io.jsonwebtoken.Jwts;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class TransactionAndSchemaAuthZTest extends AuthZTest {

    @SneakyThrows
    @BeforeClass(alwaysRun = true)
    public void setup() {
        configureTokenAuthentication();
        configureDefaultAuthorization();
        enableTransaction();
        start();
        createTransactionCoordinatorAssign(16);
        this.superUserAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .build();
        final TenantInfo tenantInfo = superUserAdmin.tenants().getTenantInfo("public");
        tenantInfo.getAdminRoles().add(TENANT_ADMIN_SUBJECT);
        superUserAdmin.tenants().updateTenant("public", tenantInfo);
        this.tenantManagerAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(TENANT_ADMIN_TOKEN))
                .build();

        superUserAdmin.tenants().createTenant("pulsar", tenantInfo);
        superUserAdmin.namespaces().createNamespace("pulsar/system");
    }

    @SneakyThrows
    @AfterClass(alwaysRun = true)
    public void cleanup() {
        close();
    }

    @BeforeMethod
    public void before() throws IllegalAccessException {
        orignalAuthorizationService = getPulsarService().getBrokerService().getAuthorizationService();
        authorizationService = Mockito.spy(orignalAuthorizationService);
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                authorizationService, true);
    }

    @AfterMethod
    public void after() throws IllegalAccessException {
        FieldUtils.writeField(getPulsarService().getBrokerService(), "authorizationService",
                orignalAuthorizationService, true);
    }

    protected void createTransactionCoordinatorAssign(int numPartitionsOfTC) throws MetadataStoreException {
        getPulsarService().getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(numPartitionsOfTC));
    }

    public enum OperationAuthType {
        Lookup,
        Produce,
        Consume,
        AdminOrSuperUser,
        NOAuth
    }

    private final String testTopic = "persistent://public/default/" + UUID.randomUUID().toString();
    @FunctionalInterface
    public interface ThrowingBiConsumer<T> {
        void accept(T t) throws PulsarAdminException;
    }

    @DataProvider(name = "authFunction")
    public Object[][] authFunction () throws Exception {
        String sub = "my-sub";
        createTopic(testTopic, false);
        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPulsarService().getBrokerServiceUrl())
                .authentication(new AuthenticationToken(SUPER_USER_TOKEN))
                .enableTransaction(true)
                .build();
        @Cleanup final Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(testTopic).create();

        @Cleanup final Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(testTopic)
                .subscriptionName(sub)
                .subscribe();

        Transaction transaction = pulsarClient.newTransaction().withTransactionTimeout(5, TimeUnit.MINUTES)
                .build().get();
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage().value("test message").send();

        consumer.acknowledgeAsync(messageId, transaction).get();

        return new Object[][]{
                // SCHEMA
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getSchemaInfo(testTopic),
                        OperationAuthType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getSchemaInfo(
                                testTopic, 0),
                        OperationAuthType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().getAllSchemas(
                                testTopic),
                        OperationAuthType.Lookup
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().createSchema(testTopic,
                                SchemaInfo.builder().type(SchemaType.STRING).build()),
                        OperationAuthType.Produce
                },
                // TODO: improve the authorization check for testCompatibility and deleteSchema
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().testCompatibility(
                                testTopic, SchemaInfo.builder().type(SchemaType.STRING).build()),
                        OperationAuthType.AdminOrSuperUser
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.schemas().deleteSchema(
                                testTopic),
                        OperationAuthType.AdminOrSuperUser
                },

                // TRANSACTION

                // Modify transaction coordinator
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .abortTransaction(transaction.getTxnID()),
                        OperationAuthType.AdminOrSuperUser
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .scaleTransactionCoordinators(17),
                        OperationAuthType.AdminOrSuperUser
                },
                // TODO: fix authorization check of check transaction coordinator stats.
                // Check transaction coordinator stats
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getCoordinatorInternalStats(1, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getCoordinatorStats(),
                        OperationAuthType.AdminOrSuperUser
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getSlowTransactionsByCoordinatorId(1, 5, TimeUnit.SECONDS),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionMetadata(transaction.getTxnID()),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .listTransactionCoordinators(),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getSlowTransactions(5, TimeUnit.SECONDS),
                        OperationAuthType.AdminOrSuperUser
                },

                // TODO: Check the authorization of the topic when get stats of TB or TP
                // Check stats related to transaction buffer and transaction pending ack
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPendingAckInternalStats(testTopic, sub, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPendingAckStats(testTopic, sub, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getPositionStatsInPendingAck(testTopic, sub, messageId.getLedgerId(),
                                        messageId.getEntryId(), null),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferInternalStats(testTopic, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferStats(testTopic, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionBufferStats(testTopic, false),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInBufferStats(transaction.getTxnID(), testTopic),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInBufferStats(transaction.getTxnID(), testTopic),
                        OperationAuthType.NOAuth
                },
                new Object[] {
                        (ThrowingBiConsumer<PulsarAdmin>) (admin) -> admin.transactions()
                                .getTransactionInPendingAckStats(transaction.getTxnID(), testTopic, sub),
                        OperationAuthType.NOAuth
                },
        };
    }

    @Test(dataProvider = "authFunction")
    public void testSchemaAndTransactionAuthorization(ThrowingBiConsumer<PulsarAdmin> adminConsumer, OperationAuthType topicOpType)
            throws Exception {
        final String subject =  UUID.randomUUID().toString();
        final String token = Jwts.builder()
                .claim("sub", subject).signWith(SECRET_KEY).compact();


        @Cleanup
        final PulsarAdmin subAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(getPulsarService().getWebServiceAddress())
                .authentication(new AuthenticationToken(token))
                .build();
        // test tenant manager
        if (topicOpType != OperationAuthType.AdminOrSuperUser) {
            adminConsumer.accept(tenantManagerAdmin);
        }

        if (topicOpType != OperationAuthType.NOAuth) {
            Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                    () -> adminConsumer.accept(subAdmin));
        }

        AtomicBoolean execFlag = null;
        if (topicOpType == OperationAuthType.Lookup) {
            execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.LOOKUP);
        } else if (topicOpType == OperationAuthType.Produce) {
            execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.PRODUCE);
        } else if (topicOpType == OperationAuthType.Consume) {
            execFlag = setAuthorizationTopicOperationChecker(subject, TopicOperation.CONSUME);
        }

        for (AuthAction action : AuthAction.values()) {
            superUserAdmin.topics().grantPermission(testTopic, subject, Set.of(action));

            if (authActionMatchOperation(topicOpType, action)) {
                adminConsumer.accept(subAdmin);
            } else {
                Assert.assertThrows(PulsarAdminException.NotAuthorizedException.class,
                        () -> adminConsumer.accept(subAdmin));
            }
            superUserAdmin.topics().revokePermissions(testTopic, subject);
        }

        if (execFlag != null) {
            Assert.assertTrue(execFlag.get());
        }

    }

    private boolean authActionMatchOperation(OperationAuthType operationAuthType, AuthAction action) {
        switch (operationAuthType) {
            case Lookup -> {
                if (AuthAction.consume == action || AuthAction.produce == action) {
                    return true;
                }
            }
            case Consume -> {
                if (AuthAction.consume == action) {
                    return true;
                }
            }
            case Produce -> {
                if (AuthAction.produce == action) {
                    return true;
                }
            }
            case AdminOrSuperUser -> {
                return false;
            }
            case NOAuth -> {
                return true;
            }
        }
        return false;
    }

}
