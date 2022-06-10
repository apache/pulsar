package org.apache.pulsar.io.mongodb;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public enum SyncType {

    /**
     * Synchronize all data.
     */
    FULL_SYNC,

    /**
     * Synchronize incremental data.
     */
    INCR_SYNC
}
