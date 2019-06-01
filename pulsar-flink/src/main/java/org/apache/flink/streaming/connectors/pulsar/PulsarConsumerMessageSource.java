package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.util.Set;

public class PulsarConsumerMessageSource<T>
        extends MessageAcknowledgingSourceBase<Message<T>, MessageId>
        implements PulsarSourceBase<Message<T>> {

    PulsarConsumerMessageSource(PulsarSourceBuilder<T> builder) {
        super(MessageId.class);

    }

    @Override
    public TypeInformation<Message<T>> getProducedType() {
        return null;
    }

    @Override
    protected void acknowledgeIDs(long l, Set<MessageId> set) {

    }

    @Override
    public void run(SourceContext<Message<T>> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }

}
