package org.apache.pulsar.storm;

import backtype.storm.tuple.Values;

/**
 * A version of PulsarTuple that *is* a Values.
 */
public class PulsarValuesTuple extends Values implements PulsarTuple {

    protected final String outputStream;

    public PulsarValuesTuple(String stream, Object ... values) {
        super(values);
        outputStream = stream;
    }

    @Override
    public String getOutputStream() {
        return outputStream;
    }

    @Override
    public Values getValues() {
        return this;
    }
}
