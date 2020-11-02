package org.apache.pulsar.functions.api.examples;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * Example function that uses the built in publish function in the context
 * to publish to a desired cluster & topic based on config.
 */
public class PublishExternalFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        // Ensure the desired cluster `external` config is provided when creating the function
        String externalCluster = "external";
        String publishTopic = (String) context.getUserConfigValueOrDefault("external-topic", "default-external-topic");
        String output = String.format("%s!", input);
        try {
            context.newOutputMessage(externalCluster, publishTopic, Schema.STRING).value(output).sendAsync();
        } catch (PulsarClientException e) {
            context.getLogger().error(e.toString());
        }
        return null;
    }
}
