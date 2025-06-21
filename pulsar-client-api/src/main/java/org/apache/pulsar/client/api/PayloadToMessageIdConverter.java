package org.apache.pulsar.client.api;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PayloadToMessageIdConverter {

    MessageId convert(LastEntry lastEntry) throws IOException;

    interface LastEntry {

        long getLedgerId();

        long getEntryId();

        int getPartitionIndex();

        /**
         * @return the buffer that can be parsed to the `MessageMetadata` defined in `PulsarApi.proto`
         */
        ByteBuffer getMetadataBuffer();

        /**
         * @return the uncompressed and unencrypted payload buffer of the last entry
         */
        ByteBuffer getPayloadBuffer();
    }
}
