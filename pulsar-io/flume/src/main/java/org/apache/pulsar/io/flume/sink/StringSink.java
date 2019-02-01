package org.apache.pulsar.io.flume.sink;


import org.apache.pulsar.functions.api.Record;

public class StringSink extends AbstractSink<String> {

    @Override
    public String extractValue(Record<String> message) {
        return message.getValue();
    }
}
