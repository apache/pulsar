package org.apache.pulsar.broker.stats.sender;

import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.stats.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

public class PulsarMetricsSender implements MetricsSender {
    private static final Logger log = LoggerFactory.getLogger(PulsarMetricsSender.class);

    private PulsarService pulsar;
    private MetricsSenderConfiguration conf;
    private ScheduledExecutorService metricsSenderExecutor;

    private TopicName topicToSend;

    private Producer<String> producer;

    public PulsarMetricsSender(PulsarService pulsar, MetricsSenderConfiguration conf) {
        this.pulsar = pulsar;
        this.conf = conf;
        this.metricsSenderExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-metrics-sender"));

        this.topicToSend = TopicName.get(
                "persistent", NamespaceName.get(this.conf.tenant, "brokers"), this.pulsar.getAdvertisedAddress());

        this.prepareTopics();

        try {
            this.producer = this.pulsar.getClient().newProducer(Schema.STRING)
                .topic(this.topicToSend.toString())
                .enableBatching(true)
                .producerName("metrics-sender-" + this.pulsar.getAdvertisedAddress())
                .create();
        } catch (PulsarClientException | PulsarServerException e) {
            e.printStackTrace();
        }
    }

    public void prepareTopics() {
        TenantInfo tenantInfo = new TenantInfo(
                pulsar.getConfig().getSuperUserRoles(),
                Sets.newHashSet(pulsar.getConfig().getClusterName()));

        try {
            this.pulsar.getAdminClient().tenants().createTenant(topicToSend.getTenant(), tenantInfo);
            this.pulsar.getAdminClient().namespaces().createNamespace(topicToSend.getNamespace());
            this.pulsar.getAdminClient().topics().createNonPartitionedTopic(topicToSend.toString());
        } catch (PulsarAdminException | PulsarServerException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        final int interval = this.conf.intervalInSeconds;
        log.info("Scheduling a thread to send metrics after [{}] seconds in background", interval);
        this.metricsSenderExecutor.scheduleAtFixedRate(safeRun(this::getAndSendMetrics), interval, interval, TimeUnit.SECONDS);
        getAndSendMetrics();
    }

    @Override
    public void getAndSendMetrics() {
        List<Metrics> metricsToSend = this.pulsar.getBrokerService().getTopicMetrics();

        metricsToSend.forEach(metrics -> {
            try {
                log.info("Sending metrics [{}]", metrics.toString());
                this.producer.send(metrics.toString());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
    }
}
