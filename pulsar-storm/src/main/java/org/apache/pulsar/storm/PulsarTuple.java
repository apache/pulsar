package org.apache.pulsar.storm;

import backtype.storm.tuple.Values;

import java.io.Serializable;

/**
 * Returned by MessageToValuesMapper, this specifies the Values
 * for an output tuple and the stream it should be sent to.
 */
public interface PulsarTuple extends Serializable {

    /**
     * Return stream the tuple should be emitted on.
     *
     * @return String
     */
    String getOutputStream();

    /**
     * Return Values for the tuple.
     *
     * @return Values
     */
    Values getValues();
}
