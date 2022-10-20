package org.apache.pulsar.broker.transaction.buffer.metadata.v2;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TxnIDData {
    /*
     * The most significant 64 bits of this TxnID.
     *
     * @serial
     */
    private long mostSigBits;

    /*
     * The least significant 64 bits of this TxnID.
     *
     * @serial
     */
    private long leastSigBits;

    @Override
    public String toString() {
        return "(" + mostSigBits + "," + leastSigBits + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mostSigBits, leastSigBits);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TxnIDData other) {
            return Objects.equals(mostSigBits, other.mostSigBits)
                    && Objects.equals(leastSigBits, other.leastSigBits);
        }

        return false;
    }
}
