package org.apache.pulsar.common.protocol;

import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CommandsTest {

    @Test
    public void initBatchMessageMetadataTest() {
        MessageMetadata messageMetadata = new MessageMetadata();
        MessageMetadata builder = new MessageMetadata();
        builder.setPublishTime(System.currentTimeMillis());
        builder.setProducerName("p1");
        builder.setSequenceId(1);
        builder.addReplicateTo("rep1");
        builder.addReplicateTo("rep2");
        builder.addProperty().setKey("key1").setValue("value1");
        builder.addProperty().setKey("key2").setValue("value2");
        Commands.initBatchMessageMetadata(messageMetadata, builder);
        Assert.assertEquals(2, messageMetadata.getPropertiesCount());
        Assert.assertEquals(2, messageMetadata.getReplicateTosCount());
    }
}
