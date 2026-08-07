# PIP-XXX: Add a broker-wide topic policies event listener

## Status

**Draft**

## Motivation

Pulsar exposes `TopicPolicyListener`, but it is scoped to one known topic:

```java
CompletableFuture<Boolean> registerListenerAsync(
        TopicName topicName,
        TopicPolicyListener listener);
```

This contract works for a loaded broker topic that needs to refresh its effective configuration. It does not support
broker extensions that need to observe topic-policy changes across the cluster because such an extension does not know
all topic names in advance. Registering only when a topic loads also misses policies for inactive or unloaded topics.

Examples of broker extensions that need an all-topic change stream include:

- metadata catalog synchronization;
- policy auditing and compliance enforcement;
- governance integrations;
- external cache invalidation; and
- operational change tracking.

Polling Pulsar Admin is expensive and introduces detection delay. A `BrokerInterceptor` callback added for each policy
operation would couple the extension point to individual admin code paths and could miss updates written by another
broker or directly observed from the policy store. Reading the namespace `__change_events` topics from a plugin would
duplicate internal routing, compatibility, and cache semantics and would not work with alternative
`TopicPoliciesService` implementations.

This PIP proposes a small, generic extension on `TopicPoliciesService`: a listener for accepted live policy changes
across every topic observed by that broker-local service. It is not by itself a cluster-wide stream: an integration
deployed on every broker can receive duplicate observations, and system-topic coverage follows the namespace readers
operated by each broker. The API contains no application-specific event or transport type. An
external broker plugin can use the notification as a change signal and perform its own processing or publication.

## Goals

- Allow an external broker extension to observe live topic-policy updates without pre-registering topic names.
- Include inactive and unloaded topics.
- Work with both built-in policy backends: namespace `__change_events` and metadata stores.
- Preserve compatibility with third-party `TopicPoliciesService` implementations.
- Isolate broker policy processing from listener failures and listener-side mutation.
- Define clear live-event, deletion, ordering, and lifecycle semantics.

## Non-goals

- Replacing the per-topic `TopicPolicyListener` used by loaded topics.
- Defining an audit log, external catalog schema, or destination topic.
- Guaranteeing durable or exactly-once delivery to a plugin.
- Replaying a snapshot when a listener registers.
- Changing topic-policy persistence formats or Admin APIs.
- Combining local and global policies into one effective-policy object.

## Public API

Add the following broker interface:

```java
@FunctionalInterface
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate
public interface TopicPoliciesEventListener {

    void onUpdate(TopicName topicName, @Nullable TopicPolicies policies);
}
```

Add two default methods to `TopicPoliciesService`:

```java
default boolean registerEventListener(TopicPoliciesEventListener listener) {
    return false;
}

default void unregisterEventListener(TopicPoliciesEventListener listener) {
}
```

The methods are defaults so an existing custom `TopicPoliciesService` remains source- and binary-compatible. Returning
`false` allows a disabled service or custom implementation to report that it does not support the capability. A plugin
must treat `false` as unsupported and avoid assuming that notifications will arrive.

## Semantics

### Live changes only

A listener receives changes accepted after registration. Registration does not emit existing cache contents, and cache
bootstrap does not emit events. An integration that needs an initial snapshot must obtain it separately and then use
these callbacks as change signals. This separation avoids a restart producing a burst that is indistinguishable from
new changes.

For the system-topic implementation, only records consumed by the live `__change_events` reader generate callbacks.
The initial compacted-topic scan and the optional per-topic listener replay do not generate callbacks.

For the metadata-store implementation, accepted create, modify, and delete notifications generate callbacks after the
relevant metadata cache has been refreshed or invalidated.

### Update and deletion

For an insert or update, `policies` is the policy object carried by that local or global policy event. For a deletion,
`policies` is `null`. Because the approved callback intentionally has only these two arguments, a tombstone does not
identify whether the local or global record was removed. Consumers that require scope or effective state must treat a
deletion as a reconciliation hint and fetch current state. `topicName` is normalized to the partitioned-topic name,
matching existing topic-policy service behavior.

Non-null local and global policy records remain separate changes and expose their scope through `TopicPolicies`. The
callback does not merge them into effective policies. A consumer that needs effective state can query the Admin API
after receiving the signal.

### Delivery and ordering

The API is a notification mechanism, not a durable log. A broker crash can lose a callback that has not completed, and
plugins must tolerate duplicate or coalesced observations by reconciling current state when necessary.

The system-topic implementation dispatches callbacks on the existing per-topic ordered policy-notification executor.
This preserves observation order for a topic and keeps plugin code off the shared system-topic reader thread. The
metadata-store implementation follows the ordering available from its metadata notifications; consumers must not infer
a total order across topics or across local and global stores.

No ordering is promised between different topics.

### Failure and mutation isolation

An exception or error thrown by one listener is logged and does not prevent other listeners from running or stop policy
cache processing. Implementations pass a separate clone of a non-null `TopicPolicies` object to each listener so one
listener cannot mutate the service cache or another listener's view.

Callbacks must return promptly and must not block the notification executor. Plugins that perform I/O should enqueue a
small immutable signal into a bounded, plugin-owned queue and process it asynchronously. Queue overflow and retry policy
belong to the plugin because the appropriate trade-off depends on its purpose.

### Registration lifecycle

Registration is process-local and remains active until explicitly unregistered or the service closes. Listener
membership is snapshotted when a change is accepted, so a listener does not receive changes accepted before it was
registered. Service close clears registrations, rejects new registrations, and suppresses callbacks still queued for
delivery. Duplicate
registration of the same listener instance is implementation-defined; plugins should register once and unregister the
same instance during shutdown.

With `LegacyAwareTopicPoliciesService`, registration succeeds only if both routed backends accept the listener. If
either backend rejects registration, the wrapper removes any partial registration and returns `false`. This prevents a
plugin from silently observing only some namespaces during a policy-storage migration.

## Implementation

### `SystemTopicBasedTopicPoliciesService`

- Store event listeners in a `CopyOnWriteArrayList`; registration is rare and notification iteration is frequent.
- After a live record updates the local or global cache and its topic name is decoded, schedule both per-topic and
  all-topic listeners on the topic's ordered notification executor.
- Do not call all-topic listeners from `initPolicesCache` or `replayTopicPolicyListeners`.
- Decode tombstone topic names from the message key, including the existing global-policy prefix.
- Clone a non-null policy separately for every callback and isolate all callback failures.
- Snapshot listener membership before scheduling asynchronous callback delivery.
- Clear listeners and reject registration after service close.

### `MetadataStoreTopicPoliciesService`

- Store event listeners in a `CopyOnWriteArrayList`.
- On deletion, invalidate the cache and notify with `null`.
- On create or modification, refresh the cache first, apply the existing local/global scope marker, and then notify.
- Clone a non-null policy separately for every callback and isolate all callback failures.
- Clear listener references when the service closes.
- Snapshot listener membership before asynchronous metadata refresh and reject registration after close.

### `LegacyAwareTopicPoliciesService`

Register the same listener with both the system-topic and configured services. Return `true` only when both registrations
succeed, rolling back a partial registration. Unregistration is forwarded to both services.

## Plugin usage example

The following illustrates lifecycle only; it does not prescribe a destination or event schema:

```java
public final class PolicyObserver implements BrokerInterceptor, TopicPoliciesEventListener {

    private TopicPoliciesService service;

    @Override
    public void initialize(PulsarService pulsarService) {
        pulsarService.runWhenReadyForIncomingRequests(() -> {
            service = pulsarService.getTopicPoliciesService();
            if (!service.registerEventListener(this)) {
                // Disable this plugin capability or report unsupported configuration.
            }
        });
    }

    @Override
    public void onUpdate(TopicName topicName, @Nullable TopicPolicies policies) {
        // Enqueue a bounded, immutable change signal; do not perform blocking I/O here.
    }

    @Override
    public void close() {
        if (service != null) {
            service.unregisterEventListener(this);
        }
    }
}
```

Registration waits for broker readiness because broker interceptors can be initialized before the configured
`TopicPoliciesService` has been constructed and started.

## Compatibility

The change adds one interface and two default methods in the broker module. It does not change wire protocols, stored
metadata, policy schemas, client APIs, or Admin APIs. Existing implementations inherit unsupported no-op behavior and
continue to load without recompilation.

The listener is `LimitedPrivate` and `Evolving`, consistent with a broker extension SPI whose contract can evolve with
appropriate compatibility care.

## Security considerations

`TopicPolicies` may reveal operational limits or configuration that a plugin should protect. This API is available only
inside the trusted broker process; it does not grant a remote caller new authorization. Operators remain responsible
for installing trusted plugins and protecting any external system to which a plugin exports data.

A slow or malicious listener can consume broker executor time. Failure isolation, off-reader dispatch, and the callback
guidance above reduce this risk, but plugin code remains trusted in-process code.

## Observability

Pulsar logs callback failures with the topic and listener identity. The core service does not add per-event metrics to
avoid unbounded listener labels and hot-path overhead. A plugin should expose its own queue depth, dropped-signal count,
processing failures, reconciliation count, and publication latency.

## Test plan

- Verify unsupported services return `false` without breaking existing implementations.
- Verify system-topic live inserts, updates, and tombstones reach all-topic listeners.
- Verify system-topic cache bootstrap and explicit per-topic replay do not reach all-topic listeners.
- Verify metadata-store create, modify, and delete notifications reach all-topic listeners.
- Verify the legacy-aware service observes namespaces routed to either backend.
- Verify listener unregistration.
- Verify callback exception isolation and defensive copies.
- Run focused broker tests and Pulsar `quickCheck`.

## Alternatives considered

### Reuse `TopicPolicyListener`

Its callback omits `TopicName`, and registration requires one known topic. Registering another listener whenever a
broker topic loads still misses policies for unloaded topics, which is the primary use case.

### Add a method to `BrokerInterceptor`

This would make topic policies a special interceptor concern and require every persistence or replication path to call
the interceptor correctly. `TopicPoliciesService` is the point that already normalizes backend changes and maintains
the accepted cache state, so it is the more coherent extension boundary.

### Let plugins consume `__change_events`

This leaks an internal storage mechanism, requires a reader per namespace, duplicates topic-key and local/global logic,
and excludes metadata-store or custom policy services.

### Poll Pulsar Admin

Polling is backend-independent but costly, delayed, and difficult to make complete for large catalogs. It is useful for
periodic reconciliation, not as the primary change signal.

### Put an external integration in Pulsar core

Core should expose a generic lifecycle and event boundary. Destination-specific schemas, credentials, retries, queues,
and delivery guarantees belong in a separately deployed plugin.

## Rollout

The capability is inactive until a listener registers. Built-in services opt in with no configuration change. External
plugins should check the boolean result, report unsupported custom services clearly, and retain periodic reconciliation
to cover process failures and other at-most-once notification gaps.
