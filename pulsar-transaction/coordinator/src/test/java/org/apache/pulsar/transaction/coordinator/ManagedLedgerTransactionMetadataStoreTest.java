package org.apache.pulsar.transaction.coordinator;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;
import org.apache.pulsar.transaction.coordinator.impl.ManagedLedgerTransactionMetadataStore;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class ManagedLedgerTransactionMetadataStoreTest extends BookKeeperClusterTestCase {

    public ManagedLedgerTransactionMetadataStoreTest() {
        super(3);
    }

    @Test
    public void testTransactionOperation() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionMetadataStore transactionMetadataStore =
                new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

        TxnID txnID = transactionMetadataStore.newTransaction(1000).get();
        Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.OPEN);

        List<String> partitions = new ArrayList<>();
        partitions.add("pt-1");
        partitions.add("pt-2");
        transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
        Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(), partitions);

        partitions.add("pt-3");
        transactionMetadataStore.addProducedPartitionToTxn(txnID, partitions).get();
        Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                partitions);

        List<TxnSubscription> subscriptions = new ArrayList<>();
        subscriptions.add(new TxnSubscription("topic1", "sub1"));
        subscriptions.add(new TxnSubscription("topic2", "sub2"));
        transactionMetadataStore.addAckedSubscriptionToTxn(txnID, subscriptions).get();
        Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                partitions);

        transactionMetadataStore.addAckedSubscriptionToTxn(txnID, subscriptions).get();
        Assert.assertEquals(transactionMetadataStore.getTxnMeta(txnID).get().producedPartitions(),
                partitions);

        transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
        Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTING);

        transactionMetadataStore.updateTxnStatus(txnID, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
        Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID).get(), TxnStatus.COMMITTED);
    }

    @Test
    public void testInitTransactionReader() throws Exception {
        ManagedLedgerFactoryConfig factoryConf = new ManagedLedgerFactoryConfig();
        factoryConf.setMaxCacheSize(0);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, zkc, factoryConf);
        TransactionMetadataStore transactionMetadataStore =
                new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

        TxnID txnID1 = transactionMetadataStore.newTransaction(1000).get();
        TxnID txnID2 = transactionMetadataStore.newTransaction(1000).get();
        Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID1).get(), TxnStatus.OPEN);
        Assert.assertEquals(transactionMetadataStore.getTxnStatus(txnID2).get(), TxnStatus.OPEN);

        List<String> partitions = new ArrayList<>();
        partitions.add("pt-1");
        partitions.add("pt-2");
        transactionMetadataStore.addProducedPartitionToTxn(txnID1, partitions).get();
        transactionMetadataStore.addProducedPartitionToTxn(txnID2, partitions).get();

        List<TxnSubscription> subscriptions = new ArrayList<>();
        subscriptions.add(new TxnSubscription("topic1", "sub1"));
        subscriptions.add(new TxnSubscription("topic2", "sub2"));

        transactionMetadataStore.addAckedSubscriptionToTxn(txnID1, subscriptions).get();
        transactionMetadataStore.addAckedSubscriptionToTxn(txnID2, subscriptions).get();
        List<TxnSubscription> subscriptions1 = new ArrayList<>();
        subscriptions1.add(new TxnSubscription("topic3", "sub3"));
        transactionMetadataStore.addAckedSubscriptionToTxn(txnID1, subscriptions1).get();
        transactionMetadataStore.addAckedSubscriptionToTxn(txnID2, subscriptions1).get();

        transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTING, TxnStatus.OPEN).get();
        transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.COMMITTING, TxnStatus.OPEN).get();

        transactionMetadataStore.updateTxnStatus(txnID1, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();
        transactionMetadataStore.updateTxnStatus(txnID2, TxnStatus.COMMITTED, TxnStatus.COMMITTING).get();

        TransactionMetadataStore transactionMetadataStoreTest =
                new ManagedLedgerTransactionMetadataStore(new TransactionCoordinatorID(1), factory);

        TxnMeta txnMeta1 = transactionMetadataStoreTest.getTxnMeta(txnID1).get();
        TxnMeta txnMeta2 = transactionMetadataStoreTest.getTxnMeta(txnID2).get();
        Assert.assertEquals(txnMeta1.producedPartitions(), partitions);
        Assert.assertEquals(txnMeta2.producedPartitions(), partitions);
        Assert.assertEquals(txnMeta1.txnSubscription(), transactionMetadataStore.getTxnMeta(txnID1).get().txnSubscription());
        Assert.assertEquals(txnMeta2.txnSubscription(), transactionMetadataStore.getTxnMeta(txnID1).get().txnSubscription());
        Assert.assertEquals(txnMeta1.status(), TxnStatus.COMMITTED);
        Assert.assertEquals(txnMeta2.status(), TxnStatus.COMMITTING);
    }
}
