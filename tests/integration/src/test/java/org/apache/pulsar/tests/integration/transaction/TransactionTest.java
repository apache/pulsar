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
package org.apache.pulsar.tests.integration.transaction;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Transaction integration test.
 */
@Slf4j
public class TransactionTest extends TransactionTestBase {

    /**
     * Transfer Business Mock Test
     *
     * The `transfer topic` represents the transfer operation, it consist of `from account`, `to account` and amount.
     * The `balance update topic` represents the account update record, it consist of account and amount.
     *
     * The transfer topic consumer receive transfer messages and produce two balance update messages,
     * one represents `from account` balance update and one represents `to account` balance update.
     *
     * example:
     *
     * receive messages:
     * transfer {
     *     fromAccount: "alice",
     *     toAccount: "bob",
     *     amount: 100
     * }
     *
     * produce messages:
     * fromAccountBalanceUpdate {
     *     account: "alice"
     *     amount: -100
     * }
     * toAccountBalanceUpdate {
     *     account: "bob",
     *     amount: 100
     * }
     *
     * test target:
     *
     * 1. The balance update messages count should be double transfer message count.
     * 2. The balance update messages amount sum should be 0.
     */
    @Test(dataProvider = "ServiceUrls")
    public void transferNormalTest(Supplier<String> serviceUrl) throws Exception {
        log.info("transfer normal test start.");
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().enableTransaction(true).serviceUrl(serviceUrl.get()).build();

        final int transferCount = 20;
        final String transferTopic = "transfer-" + randomName(6);
        final String balanceUpdateTopic = "balance-update-" + randomName(6);

        @Cleanup
        Producer<TransferOperation> transferProducer = pulsarClient
                .newProducer(Schema.JSON(TransferOperation.class))
                .topic(transferTopic)
                .create();
        log.info("transfer producer create finished");

        prepareTransferData(transferProducer, transferCount);

        @Cleanup
        Consumer<TransferOperation> transferConsumer = pulsarClient.newConsumer(Schema.JSON(TransferOperation.class))
                .topic(transferTopic)
                .subscriptionName("integration-test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared)
                .enableBatchIndexAcknowledgment(true)
                .subscribe();
        Awaitility.await().until(transferConsumer::isConnected);
        log.info("transfer consumer create finished");

        @Cleanup
        Producer<BalanceUpdate> balanceUpdateProducer = pulsarClient.newProducer(Schema.JSON(BalanceUpdate.class))
                .topic(balanceUpdateTopic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();
        log.info("balance update producer create finished");

        @Cleanup
        Consumer<BalanceUpdate> balanceUpdateConsumer = pulsarClient.newConsumer(Schema.JSON(BalanceUpdate.class))
                .topic(balanceUpdateTopic)
                .subscriptionName("integration-test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Awaitility.await().until(balanceUpdateConsumer::isConnected);
        log.info("balance update consumer create finished");

        while (true) {
            Message<TransferOperation> message = transferConsumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            TransferOperation transferOperation = message.getValue();

            Transaction transaction = pulsarClient.newTransaction()
                    .withTransactionTimeout(5, TimeUnit.MINUTES)
                    .build().get();

            balanceUpdateProducer.newMessage(transaction)
                    .value(getBalanceUpdate(transferOperation, true)).sendAsync();

            balanceUpdateProducer.newMessage(transaction)
                    .value(getBalanceUpdate(transferOperation, false)).sendAsync();

            transferConsumer.acknowledgeAsync(message.getMessageId(), transaction);

            transaction.commit().get();
        }

        int receiveBalanceUpdateCnt = 0;
        int balanceSum = 0;
        while (true) {
            Message<BalanceUpdate> message = balanceUpdateConsumer.receive(10, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            receiveBalanceUpdateCnt++;

            BalanceUpdate balanceUpdate = message.getValue();
            balanceSum += balanceUpdate.getAmount();
            log.info("balance account: {}, amount: {}", balanceUpdate.getAccount(), balanceUpdate.getAmount());
        }
        Assert.assertEquals(receiveBalanceUpdateCnt, transferCount * 2);
        Assert.assertEquals(balanceSum, 0);
        log.info("transfer normal test finish.");
    }

}
