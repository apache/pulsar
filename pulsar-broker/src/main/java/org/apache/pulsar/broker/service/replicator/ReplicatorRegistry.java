package org.apache.pulsar.broker.service.replicator;

import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.replicator.api.ReplicatorProvider;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorManager;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorProvider;

/**
 * Manages replicator's implementation mapping.
 *
 */
public class ReplicatorRegistry {

    public static String getReplicatorManagerName(ReplicatorType replicatorType) {
        switch (replicatorType) {
        case Kinesis:
            return KinesisReplicatorManager.class.getName();
        default:
            throw new RestException(Status.BAD_REQUEST, "Unknown replicator type");
        }
    }

    public static ReplicatorProvider getReplicatorProvider(ReplicatorType replicatorType) {
        switch (replicatorType) {
        case Kinesis:
            return KinesisReplicatorProvider.instance();
        default:
            throw new RestException(Status.BAD_REQUEST, "Invalid replicator type");
        }
    }

}
