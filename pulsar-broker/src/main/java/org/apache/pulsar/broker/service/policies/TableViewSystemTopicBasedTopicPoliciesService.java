package org.apache.pulsar.broker.service.policies;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Sets;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Topic policies service based on system topic with table view.
 */
@Slf4j
public class TableViewSystemTopicBasedTopicPoliciesService implements TopicPoliciesService {
    private final PulsarClient internalClient;
    private final NamespaceService namespaceService;
    // todo support key filter view to reduce memory cost
    private final Map<NamespaceName, CompletableFuture<TableView<PulsarEvent>>> views;
    private final Map<NamespaceName, CompletableFuture<Producer<PulsarEvent>>> writers;
    private final Map<TopicName, List<TopicPolicyListener<TopicPolicies>>> listeners;
    private final String localCluster;

    public TableViewSystemTopicBasedTopicPoliciesService(@Nonnull PulsarService pulsarService)
            throws PulsarServerException {
        requireNonNull(pulsarService);
        this.internalClient = pulsarService.getClient();
        this.namespaceService = pulsarService.getNamespaceService();
        this.views = new ConcurrentHashMap<>();
        this.writers = new ConcurrentHashMap<>();
        this.listeners = new ConcurrentHashMap<>();
        this.localCluster = pulsarService.getConfiguration().getClusterName();
    }

    @Override
    public @Nonnull CompletableFuture<Void> deleteTopicPoliciesAsync(@Nonnull TopicName topicName) {
        return updateTopicPoliciesAsync(topicName, null);
    }

    @Override
    public @Nonnull CompletableFuture<Void> updateTopicPoliciesAsync(@Nonnull TopicName topicName,
                                                                     @Nullable TopicPolicies policies) {
        if (topicName == null) {
            return failedFuture(new NullPointerException());
        }
        final var ns = topicName.getNamespaceObject();
        final CompletableFuture<Void> updateFuture = getOrInitWriterAsync(ns)
                .thenCompose(writer -> {
                    final var key = topicName.getPartitionedTopicName();
                    if (policies == null) {
                        return writer.newMessage().key(key).value(null).sendAsync();
                    }
                    final var builder = PulsarEvent.builder();
                    if (!policies.isGlobalPolicies()) {
                        // we don't need to replicate local policies to remote cluster, so set `replicateTo` to empty.
                        builder.replicateTo(Sets.newHashSet(localCluster));
                    }
                    final var event = builder
                            .actionType(ActionType.UPDATE)
                            .eventType(EventType.TOPIC_POLICY)
                            .topicPoliciesEvent(
                                    TopicPoliciesEvent.builder()
                                            .domain(topicName.getDomain().toString())
                                            .tenant(topicName.getTenant())
                                            .namespace(topicName.getNamespaceObject().getLocalName())
                                            .topic(TopicName.get(topicName.getPartitionedTopicName()).getLocalName())
                                            .policies(policies)
                                            .build())
                            .build();
                    return writer.newMessage().key(key).value(event).sendAsync();
                }).thenApply(__ -> null);
        updateFuture.exceptionally(ex -> {
            // auto-recovery
            cleanupWriter(ns);
            return null;
        });
        return updateFuture;
    }

    @Override
    public @Nullable TopicPolicies getTopicPolicies(@Nonnull TopicName topicName)
            throws BrokerServiceException.TopicPoliciesCacheNotInitException {
        requireNonNull(topicName);
        final var policiesFuture = getTopicPoliciesAsync(topicName);
        // using retry to implement async-like logic
        if (!policiesFuture.isDone() && policiesFuture.isCompletedExceptionally()) {
            throw new BrokerServiceException.TopicPoliciesCacheNotInitException();
        }
        return policiesFuture.join().orElse(null);
    }

    @Override
    public @Nullable TopicPolicies getTopicPoliciesIfExists(@Nonnull TopicName topicName) {
        requireNonNull(topicName);
        var policiesFuture = getTopicPoliciesAsync(topicName);
        if (!policiesFuture.isDone() && policiesFuture.isCompletedExceptionally()) {
            return null;
        }
        return policiesFuture.join().orElse(null);
    }

    @Override
    public @Nullable TopicPolicies getTopicPolicies(@Nonnull TopicName topicName, boolean isGlobal)
            throws BrokerServiceException.TopicPoliciesCacheNotInitException {
        var topicPolicies = getTopicPolicies(topicName);
        if (topicPolicies == null) {
            return null;
        }
        if (topicPolicies.isGlobalPolicies() != isGlobal) {
            return null;
        }
        return topicPolicies;
    }

    @Override
    public @Nonnull CompletableFuture<TopicPolicies> getTopicPoliciesBypassCacheAsync(@Nonnull TopicName topicName) {
        if (topicName == null) {
            return failedFuture(new NullPointerException());
        }
        return getTopicPoliciesAsync(topicName)
                .thenApply(topicPolicies -> topicPolicies.orElse(null));
    }


    private @NotNull CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName) {
        var ns = topicName.getNamespaceObject();
        var viewFuture = getOrInitViewAsync(ns);
        final CompletableFuture<Optional<TopicPolicies>> policiesFuture = viewFuture.thenApply(view -> {
            var event = view.get(topicName.getPartitionedTopicName());
            if (event == null || event.getEventType() != EventType.TOPIC_POLICY) {
                return Optional.empty();
            }
            var topicPoliciesEvent = event.getTopicPoliciesEvent();
            if (topicPoliciesEvent == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(topicPoliciesEvent.getPolicies());
        });
        policiesFuture.exceptionally(ex -> {
            cleanupView(ns);
            return Optional.empty();
        });
        return policiesFuture;
    }

    @Override
    public @Nonnull CompletableFuture<Void> addOwnedNamespaceBundleAsync(@Nonnull NamespaceBundle bundle) {
        if (bundle == null) {
            return failedFuture(new NullPointerException());
        }
        final var ns = bundle.getNamespaceObject();
        return getOrInitViewAsync(ns).thenApply(__ -> null);
    }

    @Override

    public @Nonnull CompletableFuture<Void> removeOwnedNamespaceBundleAsync(@Nonnull NamespaceBundle bundle) {
        final var ns = bundle.getNamespaceObject();
        return namespaceService.getOwnedBundles(ns)
                .thenCompose(ownedBundles -> {
                    if (ownedBundles.size() != 0) {
                        // don't need to clean up view
                        return completedFuture(null);
                    }
                    final List<CompletableFuture<Void>> container = Lists.newArrayListWithCapacity(2);
                    container.add(cleanupView(ns));
                    container.add(cleanupWriter(ns));
                    return FutureUtil.waitForAll(container);
                });
    }

    @Override
    public void start() {
        namespaceService.addNamespaceBundleOwnershipListener(
                new NamespaceBundleOwnershipListener() {
                    @Override
                    public void onLoad(NamespaceBundle bundle) {
                        addOwnedNamespaceBundleAsync(bundle)
                                .exceptionally(ex -> {
                                    log.warn("Exception occur while trying init "
                                                    + "topic policies by namespace onload event. namespace: {}",
                                            bundle.getNamespaceObject(), ex);
                                    return null;
                                });
                    }

                    @Override
                    public void unLoad(NamespaceBundle bundle) {
                        removeOwnedNamespaceBundleAsync(bundle)
                                .exceptionally(ex -> {
                                    log.warn("Exception occur while trying cleanup "
                                                    + "topic policies by namespace unloading event. namespace: {}",
                                            bundle.getNamespaceObject(), ex);
                                    return null;
                                });
                    }

                    @Override
                    public boolean test(NamespaceBundle namespaceBundle) {
                        return true;
                    }
                });
    }

    @Override
    public void registerListener(@Nonnull TopicName topicName, @Nonnull TopicPolicyListener<TopicPolicies> listener) {
        requireNonNull(topicName);
        requireNonNull(listener);
        listeners.computeIfAbsent(topicName, (tp) -> new CopyOnWriteArrayList<>()).add(listener);
    }

    @Override
    public void unregisterListener(@Nonnull TopicName topicName, @Nonnull TopicPolicyListener<TopicPolicies> listener) {
        requireNonNull(topicName);
        requireNonNull(listener);
        listeners.computeIfPresent(topicName, (tp, listeners) -> {
            listeners.remove(listener);
            if (listeners.size() == 0) { // cleanup the listeners
                return null;
            }
            return listeners;
        });
    }

    private @Nonnull CompletableFuture<Void> cleanupView(@Nonnull NamespaceName ns) {
        final var viewFuture = views.remove(ns);
        if (!viewFuture.isDone()) {
            viewFuture.cancel(true);
            return completedFuture(null);
        }
        if (!viewFuture.isCompletedExceptionally()) {
            // don't need to close the resource of view
            return completedFuture(null);
        }
        final var view = viewFuture.join();
        return view.closeAsync();
    }

    private @Nonnull CompletableFuture<Void> cleanupWriter(@Nonnull NamespaceName ns) {
        final var writerFuture = writers.remove(ns);
        if (!writerFuture.isDone()) {
            writerFuture.cancel(true);
            return completedFuture(null);
        }
        if (!writerFuture.isCompletedExceptionally()) {
            // don't need to close the resource of writer
            return completedFuture(null);
        }
        final var writer = writerFuture.join();
        return writer.closeAsync();
    }

    private @Nonnull CompletableFuture<TableView<PulsarEvent>> getOrInitViewAsync(@Nonnull NamespaceName ns) {
        return views.computeIfAbsent(ns, (namespace) -> internalClient.newTableView(Schema.AVRO(PulsarEvent.class))
                .topic(getEventTopic(namespace))
                .createAsync())
                .thenApply(view -> {
                    view.forEachAndListen(this::listenerProcessor);
                    return view;
                });
    }


    private void listenerProcessor(@Nonnull String key, @Nullable PulsarEvent event) {

        // Note: Table view will call this method internally, If there are performance problem, we can try
        //       moving the listener out of table view then use multi thread to invoke callback.
        try {
            if (event == null) {
                return;
            }
            if (event.getEventType() != EventType.TOPIC_POLICY) {
                return;
            }
            final var partitionParentTopicName = TopicName.get(key);
            var listenerList = listeners.get(partitionParentTopicName);
            if (listenerList == null || listenerList.isEmpty()) {
                return;
            }
            listenerList.forEach(listener -> {
                try {
                    listener.onUpdate(event.getTopicPoliciesEvent().getPolicies());
                } catch (Throwable ex) {
                    // avoid listener affect all of listeners
                    log.warn("Error occur while trying callback listener. event_key: {}," +
                                    " event: {} listener :{}", key, event, listener.toString());
                }
            });
        } catch (Throwable ex) {
            // avoid listener affect broker
            log.warn("Error occur while trying callback listener. event_key: {}, event: {}",
                    key, event);
        }
    }

    private @Nonnull CompletableFuture<Producer<PulsarEvent>> getOrInitWriterAsync(@Nonnull NamespaceName ns) {
        return writers.computeIfAbsent(ns, (namespace) -> internalClient.newProducer(Schema.AVRO(PulsarEvent.class))
                .topic(getEventTopic(namespace))
                .enableBatching(false)
                .createAsync());
    }

    private static @Nonnull String getEventTopic(@Nonnull NamespaceName namespace) {
        return "persistent://" + namespace + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
    }
}
