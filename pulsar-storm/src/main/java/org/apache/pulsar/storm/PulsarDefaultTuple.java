package org.apache.pulsar.storm;

import backtype.storm.tuple.Values;

/**
 * A PulsarTuple that just uses the default stream.
 */
public class PulsarDefaultTuple implements PulsarTuple {

    protected final Values values;

    public PulsarDefaultTuple(Values values) {
        this.values = values;
    }

    @Override
    public String getOutputStream() {
        return "default";
    }

    @Override
    public Values getValues() {
        return values;
    }
}
