package org.apache.pulsar.broker.service;

import lombok.Value;

@Value
public class ThrottledPublishSource {
    PublishSource publishSource;
    Runnable unthrottleCallback;

    public void unthrottle() {
        unthrottleCallback.run();
    }
}
