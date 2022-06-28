package org.apache.pulsar.broker.transaction.buffer;

import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState;
import org.apache.pulsar.transaction.coordinator.impl.MLTransactionMetadataStore;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicTransactionBufferTest extends TransactionTestBase {


    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        setBrokerCount(1);
        setUpBase(1, 16, "persistent://" + NAMESPACE1 + "/test", 0);

        Map<TransactionCoordinatorID, TransactionMetadataStore> stores =
                getPulsarServiceList().get(0).getTransactionMetadataStoreService().getStores();
        Awaitility.await().until(() -> {
            if (stores.size() == 16) {
                for (TransactionCoordinatorID transactionCoordinatorID : stores.keySet()) {
                    if (((MLTransactionMetadataStore) stores.get(transactionCoordinatorID)).getState()
                            != TransactionMetadataStoreState.State.Ready) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        });
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTransactionBufferAppendMarkerWriteFailState() throws Exception {
        final String topic = "persistent://" + NAMESPACE1 + "/testPendingAckManageLedgerWriteFailState";
        Transaction txn = pulsarClient.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        producer.newMessage(txn).value("test".getBytes()).send();
        PersistentTopic persistentTopic = (PersistentTopic) getPulsarServiceList().get(0)
                .getBrokerService().getTopic(TopicName.get(topic).toString(), false).get().get();
        Whitebox.setInternalState(persistentTopic.getManagedLedger(), "state", ManagedLedgerImpl.State.WriteFailed);
        txn.commit().get();
    }
}
