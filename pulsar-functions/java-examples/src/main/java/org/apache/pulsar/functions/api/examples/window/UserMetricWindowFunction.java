package org.apache.pulsar.functions.api.examples.window;

import java.util.Collection;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;

/**
 * Example function that wants to keep track of
 * the event time of each message sent.
 */
public class UserMetricWindowFunction implements WindowFunction<String, Void> {

    @Override
    public Void process(Collection<Record<String>> inputs, WindowContext context) throws Exception {

        for (Record<String> record : inputs) {
            if (record.getEventTime().isPresent()) {
                context.recordMetric("MessageEventTime", record.getEventTime().get().doubleValue());
            }
        }

        return null;
    }
}
