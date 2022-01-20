package org.apache.pulsar.client.impl;

import lombok.Data;
import org.apache.pulsar.client.api.Progress;

@Data
public class ProgressInfo {
    private Progress progress;
    private long sequenceId;
}
