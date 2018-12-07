package org.apache.pulsar.common.api.raw;

public class RawMessageIdImpl implements RawMessageId {

    long ledgerId;
    long entryId;
    long batchIndex;

    @Override
    public String toString() {
        return new StringBuilder()
                .append('(')
                .append(ledgerId)
                .append(',')
                .append(entryId)
                .append(',')
                .append(batchIndex)
                .append(')')
                .toString();
    }
}
