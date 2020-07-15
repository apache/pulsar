package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageIdData;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.testng.annotations.Test;

public class RepeatedLongTest {

    @Test
    public void testMessageIDAckSet() throws Exception {
        MessageIdData messageIdData = MessageIdData.newBuilder()
            .setLedgerId(0L)
            .setEntryId(0L)
            .setPartition(0)
            .setBatchIndex(0)
            .addAckSet(1000)
            .addAckSet(1001)
            .addAckSet(1003)
            .build();

        int cmdSize = messageIdData.getSerializedSize();
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(cmdSize);
        ByteBufCodedOutputStream outputStream = ByteBufCodedOutputStream.get(buf);
        messageIdData.writeTo(outputStream);

        messageIdData.recycle();
        outputStream.recycle();

        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(buf);
        MessageIdData newMessageIdData = MessageIdData.newBuilder()
            .mergeFrom(inputStream, null)
            .build();
        inputStream.recycle();

        assertEquals(3, newMessageIdData.getAckSetCount());
        assertEquals(1000, newMessageIdData.getAckSet(0));
        assertEquals(1001, newMessageIdData.getAckSet(1));
        assertEquals(1003, newMessageIdData.getAckSet(2));
        newMessageIdData.recycle();
    }

}
