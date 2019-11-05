package org.apache.pulsar.transaction.util;

import static org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus.ABORTED;
import static org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus.ABORTING;
import static org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus.COMMITTED;
import static org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus.COMMITTING;

import org.apache.pulsar.common.api.proto.PulsarApi.TxnStatus;


/**
 * An transaction util of {@link TransactionUtil}.
 */
public class TransactionUtil {
    /**
     * Check if the a status can be transaction to a new status.
     *
     * @param newStatus the new status
     * @return true if the current status can be transitioning to.
     */
    public static boolean canTransitionTo(TxnStatus currentStatus, TxnStatus newStatus) {

        switch (currentStatus) {
            case OPEN:
                return newStatus != COMMITTED && newStatus != ABORTED;
            case COMMITTING:
                return newStatus == COMMITTING || newStatus == COMMITTED;
            case COMMITTED:
                return newStatus == COMMITTED;
            case ABORTING:
                return newStatus == ABORTING || newStatus == ABORTED;
            case ABORTED:
                return newStatus == ABORTED;
            default:
                throw new IllegalArgumentException("Unknown txn status : " + newStatus);
        }
    }
}
