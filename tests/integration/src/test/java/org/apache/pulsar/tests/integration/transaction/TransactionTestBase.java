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
package org.apache.pulsar.tests.integration.transaction;

import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;

/**
 * Transaction test base.
 */
@Slf4j
public abstract class TransactionTestBase extends PulsarTestSuite {

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            brokerContainer.withEnv("transactionCoordinatorEnabled", "true");
            brokerContainer.withEnv("transactionBufferProviderClassName",
                    "org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBufferProvider");
            brokerContainer.withEnv("acknowledgmentAtBatchIndexLevelEnabled", "true");
        }
    }

    private void transactionCoordinatorMetadataInitialize() throws Exception {
        BrokerContainer brokerContainer = pulsarCluster.getBrokers().iterator().next();
        ContainerExecResult result = brokerContainer.execCmd(
                "/pulsar/bin/pulsar", "initialize-transaction-coordinator-metadata",
                "-cs", ZKContainer.NAME,
                "-c", pulsarCluster.getClusterName());
    }

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        transactionCoordinatorMetadataInitialize();
    }

    public void prepareTransferData(Producer<TransferOperation> transferProducer, int messageCnt) {
        for (int i = 0; i < messageCnt; i++) {
            TransferOperation transferOperation = new TransferOperation();
            transferOperation.setFromAccount("alice");
            transferOperation.setToAccount("Bob");
            transferOperation.setAmount(100 * i);
            CompletableFuture<MessageId> completableFuture =
                    transferProducer.newMessage().value(transferOperation).sendAsync();
        }
        log.info("transfer messages produced");
    }

    public BalanceUpdate getBalanceUpdate(TransferOperation transferOperation, boolean isFromAccount) {
        BalanceUpdate balanceUpdate = new BalanceUpdate();
        balanceUpdate.setAccount(transferOperation.getFromAccount());
        if (isFromAccount) {
            balanceUpdate.setAmount(-(transferOperation.getAmount()));
        } else {
            balanceUpdate.setAmount(transferOperation.getAmount());
        }
        return balanceUpdate;
    }

    @Data
    public static class TransferOperation {
        private String fromAccount;
        private String toAccount;
        private int amount;
    }

    @Data
    public static class BalanceUpdate {
        private String account;
        private int amount;
    }

}
