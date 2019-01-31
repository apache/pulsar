package org.apache.pulsar.io.flume.source;


public class StringSource extends AbstractSource<String> {

    @Override
    public String extractValue(String message) {
        return message;
    }
}
