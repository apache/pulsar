package org.apache.pulsar.transaction.coordinator.impl;

import java.util.Objects;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/***
 * The difference with {@link PositionImpl} is that there are two more parameters:
 * {@link #batchSize}, {@link #batchIndex}.
 */
public class TxnBatchedPositionImpl extends PositionImpl {

    /** The data length of current batch. **/
    @Getter
    private final int batchSize;

    /** The position of current batch. **/
    @Getter
    private final int batchIndex;

    public TxnBatchedPositionImpl(Position position, int batchSize, int batchIndex, long[] ackSet){
        super(position.getLedgerId(), position.getEntryId(), ackSet);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TxnBatchedPositionImpl other) {
            return super.equals(o) && batchSize == other.batchSize && batchIndex == other.batchIndex;
        }
        return false;

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), batchSize, batchIndex);
    }
}
