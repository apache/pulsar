package org.apache.pulsar.storm;


import org.apache.storm.tuple.Values;

/**
 * Returned by MessageToValuesMapper, this specifies the Values
 * for an output tuple and the stream it should be sent to.
 */
public class PulsarTuple extends Values {

    protected final String outputStream;

    public PulsarTuple(String outStream, Object ... values) {
        super(values);
        outputStream = outStream;
    }

    /**
     * Return stream the tuple should be emitted on.
     *
     * @return String
     */
    public String getOutputStream() {
        return outputStream;
    }
}
