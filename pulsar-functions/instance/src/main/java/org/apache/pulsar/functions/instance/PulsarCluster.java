package org.apache.pulsar.functions.instance;

import lombok.Getter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.functions.proto.Function.ProducerSpec;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.source.TopicSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class PulsarCluster {
    @Getter
    private PulsarClient client;

    @Getter
    private final TopicSchema topicSchema;

    @Getter
    private ProducerBuilderImpl<?> producerBuilder;

    @Getter
    private Map<String, Producer<?>> publishProducers;

    @Getter
    private ThreadLocal<Map<String, Producer<?>>> tlPublishProducers;

    public PulsarCluster(PulsarClient client, ProducerSpec producerSpec) {
        this.client = client;
        this.topicSchema = new TopicSchema(client);
        this.producerBuilder = (ProducerBuilderImpl<?>) client.newProducer().blockIfQueueFull(true).enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        boolean useThreadLocalProducers = false;
        if (producerSpec != null) {
            if (producerSpec.getMaxPendingMessages() != 0) {
                this.producerBuilder.maxPendingMessages(producerSpec.getMaxPendingMessages());
            }
            if (producerSpec.getMaxPendingMessagesAcrossPartitions() != 0) {
                this.producerBuilder.maxPendingMessagesAcrossPartitions(producerSpec.getMaxPendingMessagesAcrossPartitions());
            }
            useThreadLocalProducers = producerSpec.getUseThreadLocalProducers();
        }
        if (useThreadLocalProducers) {
            tlPublishProducers = new ThreadLocal<>();
        } else {
            publishProducers = new HashMap<>();
        }
    }
}
