package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class RestProduceMessage {

    // Key of the message for routing policy.
    private String key;

    // Encoded message body.
    private String value;

    // The partition to publish the message to.
    private int partition;

    // The properties of the message.
    private Map<String, String> properties;

    // The event time of the message.
    private long eventTime;

    // The sequence id of the message.
    private long sequenceId;

    // The list of clusters to replicate.
    private List<String> replicationClusters;

    // The flag to disable replication.
    private boolean disableReplication;

    // Deliver the message only at or after the specified absolute timestamp.
    private long deliverAt;

    // Deliver the message only after the specified relative delay in milliseconds.
    private long deliverAfterMs;
}
