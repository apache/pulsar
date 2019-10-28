package org.apache.pulsar.transaction.coordinator.reader;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.impl.TopicBaseTransactionMetadataStore;
import org.apache.pulsar.transaction.impl.common.TxnID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TopicBaseTransactionReader implements
        TopicBaseTransactionMetadataStore.TopicBaseTransactionMetadataReader {

    private final ConcurrentHashMap<TxnID, TxnMeta> tnxMetaMap = new ConcurrentHashMap<>();

    private final Reader<byte[]> reader;

    private final String topicName;

    private final PulsarClient pulsarClient;


    public TopicBaseTransactionReader(String topicName, PulsarClient pulsarClient) throws PulsarClientException {
        this.topicName = topicName;
        this.pulsarClient = pulsarClient;
        this.reader = pulsarClient
                .newReader(Schema.BYTES)
                .topic(topicName)
                .startMessageIdInclusive()
                .startMessageId(MessageId.earliest)
                .create();
        while(reader.hasMessageAvailable()) {
            reader.readNext().getValue();

        }

    }


    @Override
    public CompletableFuture<TxnMeta> read(TxnID txnid) {
        return null;
    }

    @Override
    public Long readSequenceId() {
        return null;
    }

    @Override
    public CompletableFuture<Void> flushCache(TxnMeta txnMeta) {
        return null;
    }
}
