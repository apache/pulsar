package org.apache.pulsar.broker.stats.sender;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.ZkAdminPaths;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerCacheMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.broker.stats.prometheus.NamespaceStatsAggregator;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES_ROOT;

public class PulsarMetricsSender implements MetricsSender {
    private static final Logger log = LoggerFactory.getLogger(PulsarMetricsSender.class);

    private PulsarService pulsar;
    private MetricsSenderConfiguration conf;
    private ScheduledExecutorService metricsSenderExecutor;

    private TopicName topicToSend;

    private Producer<PulsarMetrics> producer;

    public PulsarMetricsSender(PulsarService pulsar, MetricsSenderConfiguration conf) {
        this.pulsar = pulsar;
        this.conf = conf;
        this.metricsSenderExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("pulsar-metrics-sender"));

        this.topicToSend = TopicName.get(
                "persistent", NamespaceName.get(this.conf.tenant, "brokers"), this.pulsar.getAdvertisedAddress());

        this.prepareTopics();
    }

    public void prepareTopics() {
        try {
            ZooKeeper zk = this.pulsar.getZkClient();
            String cluster = this.pulsar.getConfig().getClusterName();

            createTenantIfAbsent(zk, topicToSend.getTenant(), cluster);
            createNamespaceIfAbsent(zk, topicToSend.getNamespaceObject(), cluster);
            createPartitionedTopic(zk, topicToSend, 1);
        } catch (PulsarServerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        try {
            this.producer = this.pulsar.getClient().newProducer(Schema.JSON(PulsarMetrics.class))
                    .topic(this.topicToSend.toString())
                    .enableBatching(true)
                    .producerName("metrics-sender-" + this.pulsar.getAdvertisedAddress())
                    .create();
        } catch (PulsarClientException | PulsarServerException e) {
            e.printStackTrace();
        }

        //metricsToSend.addAll(new ManagedLedgerCacheMetrics(this.pulsar).generate());
        //metricsToSend.addAll(new ManagedLedgerMetrics(pulsar).generate());
        //metricsToSend.addAll(pulsar.getLoadManager().get().getLoadBalancingMetrics());
        //List<Metrics> yo = this.pulsar.getBrokerService().getTopicMetrics();
        //metricsToSend.addAll(this.pulsar.getBrokerService().getTopicMetrics());

        final int initialDelay = 1;
        final int interval = this.conf.intervalInSeconds;
        log.info("Scheduling a thread to send metrics after [{}] seconds in background", interval);
        this.metricsSenderExecutor.scheduleAtFixedRate(safeRun(this::generateAndSend), initialDelay, interval, TimeUnit.SECONDS);
    }

    private void generateAndSend() {
        NamespaceStatsAggregator.generate(this.pulsar, true, true, this);
    }

    @Override
    public void send(PulsarMetrics pulsarMetrics) {
        try {
            this.producer.send(pulsarMetrics);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        this.metricsSenderExecutor.shutdown();
    }

    static void createTenantIfAbsent(ZooKeeper configStoreZk, String tenant, String cluster) throws IOException,
            KeeperException, InterruptedException {

        String tenantPath = POLICIES_ROOT + "/" + tenant;

        Stat stat = configStoreZk.exists(tenantPath, false);
        if (stat == null) {
            TenantInfo publicTenant = new TenantInfo(Collections.emptySet(), Collections.singleton(cluster));

            createZkNode(configStoreZk, tenantPath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            // Update existing public tenant with new cluster
            byte[] content = configStoreZk.getData(tenantPath, false, null);
            TenantInfo publicTenant = ObjectMapperFactory.getThreadLocal().readValue(content, TenantInfo.class);

            // Only update z-node if the list of clusters should be modified
            if (!publicTenant.getAllowedClusters().contains(cluster)) {
                publicTenant.getAllowedClusters().add(cluster);

                configStoreZk.setData(tenantPath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(publicTenant),
                        stat.getVersion());
            }
        }
    }

    static void createNamespaceIfAbsent(ZooKeeper configStoreZk, NamespaceName namespaceName, String cluster)
            throws KeeperException, InterruptedException, IOException {
        String namespacePath = POLICIES_ROOT + "/" +namespaceName.toString();
        Policies policies;
        Stat stat = configStoreZk.exists(namespacePath, false);
        if (stat == null) {
            policies = new Policies();
            policies.bundles = getBundles(16);
            policies.replication_clusters = Collections.singleton(cluster);

            createZkNode(
                    configStoreZk,
                    namespacePath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } else {
            byte[] content = configStoreZk.getData(namespacePath, false, null);
            policies = ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);

            // Only update z-node if the list of clusters should be modified
            if (!policies.replication_clusters.contains(cluster)) {
                policies.replication_clusters.add(cluster);

                configStoreZk.setData(namespacePath, ObjectMapperFactory.getThreadLocal().writeValueAsBytes(policies),
                        stat.getVersion());
            }
        }
    }

    static void createPartitionedTopic(ZooKeeper configStoreZk, TopicName topicName, int numPartitions) throws KeeperException, InterruptedException, IOException {
        String partitionedTopicPath = ZkAdminPaths.partitionedTopicPath(topicName);
        Stat stat = configStoreZk.exists(partitionedTopicPath, false);
        PartitionedTopicMetadata metadata = new PartitionedTopicMetadata(numPartitions);
        if (stat == null) {
            createZkNode(
                    configStoreZk,
                    partitionedTopicPath,
                    ObjectMapperFactory.getThreadLocal().writeValueAsBytes(metadata),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        } else {
            byte[] content = configStoreZk.getData(partitionedTopicPath, false, null);
            PartitionedTopicMetadata existsMeta = ObjectMapperFactory.getThreadLocal().readValue(content, PartitionedTopicMetadata.class);

            // Only update z-node if the partitions should be modified
            if (existsMeta.partitions < numPartitions) {
                configStoreZk.setData(
                        partitionedTopicPath,
                        ObjectMapperFactory.getThreadLocal().writeValueAsBytes(metadata),
                        stat.getVersion()
                );
            }
        }
    }

    /**
     * a wrapper for ZkUtils.createFullPathOptimistic but ignore exception of node exists
     */
    private static void createZkNode(ZooKeeper zkc, String path, byte[] data, final List<ACL> acl,
                                     final CreateMode createMode) throws KeeperException, InterruptedException {
        try {
            ZkUtils.createFullPathOptimistic(zkc, path, data, acl, createMode);
        } catch (KeeperException.NodeExistsException e) {
            e.printStackTrace();
        }
    }

    private static BundlesData getBundles(int numBundles) {
        Long maxVal = ((long) 1) << 32;
        Long segSize = maxVal / numBundles;
        List<String> partitions = Lists.newArrayList();
        partitions.add(String.format("0x%08x", 0l));
        Long curPartition = segSize;
        for (int i = 0; i < numBundles; i++) {
            if (i != numBundles - 1) {
                partitions.add(String.format("0x%08x", curPartition));
            } else {
                partitions.add(String.format("0x%08x", maxVal - 1));
            }
            curPartition += segSize;
        }
        return new BundlesData(partitions);
    }
}
