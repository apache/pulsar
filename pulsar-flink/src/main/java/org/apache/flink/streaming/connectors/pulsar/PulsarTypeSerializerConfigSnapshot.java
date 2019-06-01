package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;

public class PulsarTypeSerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot {

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int getVersion() {
        return 0;
    }
}
