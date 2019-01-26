package org.apache.pulsar.io.flume;


public class FlumeStringSource extends FlumeAbstractSource<String> {

    @Override
    public String extractValue(String message) {
        return message;
    }
}
