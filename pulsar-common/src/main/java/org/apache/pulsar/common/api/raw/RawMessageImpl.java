package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;

public class RawMessageImpl implements RawMessage {

    private final RawMessageIdImpl messageId = new RawMessageIdImpl();

    private MessageMetadata msgMetadata;
    private PulsarApi.SingleMessageMetadata.Builder singleMessageMetadata;
    private ByteBuf payload;

    private static final Recycler<RawMessageImpl> RECYCLER = new Recycler<RawMessageImpl>() {
        @Override
        protected RawMessageImpl newObject(Handle<RawMessageImpl> handle) {
            return new RawMessageImpl(handle);
        }
    };

    private final Handle<RawMessageImpl> handle;

    private RawMessageImpl(Handle<RawMessageImpl> handle) {
        this.handle = handle;
    }

    @Override
    public void release() {
        if (singleMessageMetadata != null) {
            singleMessageMetadata.recycle();
            singleMessageMetadata = null;
        }

        payload.release();
        handle.recycle(this);
    }

    public static RawMessage get(MessageMetadata msgMetadata,
            PulsarApi.SingleMessageMetadata.Builder singleMessageMetadata,
            ByteBuf payload,
            long ledgerId, long entryId, long batchIndex) {
        RawMessageImpl msg = RECYCLER.get();
        msg.msgMetadata = msgMetadata;
        msg.singleMessageMetadata = singleMessageMetadata;
        msg.messageId.ledgerId = ledgerId;
        msg.messageId.entryId = entryId;
        msg.messageId.batchIndex = batchIndex;
        msg.payload = payload;
        return msg;
    }

    @Override
    public Map<String, String> getProperties() {
        if (singleMessageMetadata != null && singleMessageMetadata.getPropertiesCount() > 0) {
            return singleMessageMetadata.getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        } else if (msgMetadata.getPropertiesCount() > 0) {
            return msgMetadata.getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public ByteBuf getData() {
        return payload;
    }

    @Override
    public RawMessageId getMessageId() {
        return messageId;
    }

    @Override
    public long getPublishTime() {
        return msgMetadata.getPublishTime();
    }

    @Override
    public long getEventTime() {
        if (singleMessageMetadata != null && singleMessageMetadata.hasEventTime()) {
            return singleMessageMetadata.getEventTime();
        } else if (msgMetadata.hasEventTime()) {
            return msgMetadata.getEventTime();
        } else {
            return 0;
        }
    }

    @Override
    public long getSequenceId() {
        return msgMetadata.getSequenceId() + messageId.batchIndex;
    }

    @Override
    public String getProducerName() {
        return msgMetadata.getProducerName();
    }

    @Override
    public Optional<String> getKey() {
        if (singleMessageMetadata != null && singleMessageMetadata.hasPartitionKey()) {
            return Optional.of(singleMessageMetadata.getPartitionKey());
        } else if (msgMetadata.hasPartitionKey()){
            return Optional.of(msgMetadata.getPartitionKey());
        } else {
            return Optional.empty();
        }
    }
}
