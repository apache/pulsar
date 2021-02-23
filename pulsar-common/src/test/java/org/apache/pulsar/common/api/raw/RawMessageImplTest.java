package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import junit.framework.TestCase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class RawMessageImplTest extends TestCase {

    private static final String HARD_CODE_KEY = "__pfn_input_topic__";
    private static final String KEY_VALUE_FIRST= "persistent://first-tenant-value/first-namespace-value/first-topic-value";
    private static final String KEY_VALUE_SECOND = "persistent://second-tenant-value/second-namespace-value/second-topic-value";
    private static final String HARD_CODE_KEY_ID = "__pfn_input_msg_id__";
    private static final String HARD_CODE_KEY_ID_VALUE  = "__pfn_input_msg_id_value__";
    public void testGetProperties() {
        ReferenceCountedMessageMetadata refCntMsgMetadata = ReferenceCountedMessageMetadata.get();
        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY).setValue(KEY_VALUE_FIRST);
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY).setValue(KEY_VALUE_SECOND);
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY_ID).setValue(HARD_CODE_KEY_ID_VALUE);
        RawMessage msg = RawMessageImpl.get(refCntMsgMetadata, singleMessageMetadata, null , 0, 0, 0);
        Map<String, String> properties = msg.getProperties();
        assertEquals(KEY_VALUE_SECOND, properties.get(HARD_CODE_KEY));
        assertEquals(HARD_CODE_KEY_ID_VALUE, properties.get(HARD_CODE_KEY_ID));
    }
}