package org.apache.pulsar.broker.service.persistent;

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarService;

public class TransactionMessageDeduplication extends MessageDeduplication {

    public TransactionMessageDeduplication(PulsarService pulsar, PersistentTopic topic, ManagedLedger managedLedger) {
        super(pulsar, topic, managedLedger);
    }

}
