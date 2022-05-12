package org.apache.pulsar.functions.transforms;

public interface TransformStep {
    void process(TransformRecord transformRecord);
}
