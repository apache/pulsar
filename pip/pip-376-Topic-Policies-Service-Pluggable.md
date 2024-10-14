# PIP-376: Make Topic Policies Service Pluggable

## Background

### Topic Policies Service and System Topics

[PIP-39](https://github.com/apache/pulsar/wiki/PIP-39%3A-Namespace-Change-Events) introduces system topics and topic-level policies. Currently, the topic policies service (`TopicPoliciesService`) has only one implementation (`SystemTopicBasedTopicPoliciesService`) that depends on system topics. Therefore, the following configurations are required (though they are enabled by default):

```properties
systemTopicEnabled=true
topicLevelPoliciesEnabled=true
```

However, using system topics to manage topic policies may not always be the best choice. Users might need an alternative approach to manage topic policies.

### Issues with the Current `TopicPoliciesService` Interface

The `TopicPoliciesService` interface is poorly designed for third-party implementations due to the following reasons:

1. **Methods that Should Not Be Exposed**:
   - `addOwnedNamespaceBundleAsync` and `removeOwnedNamespaceBundleAsync` are used internally in `SystemTopicBasedTopicPoliciesService`.
   - `getTopicPoliciesBypassCacheAsync` is used only in tests to replay the `__change_events` topic and construct the topic policies map.

2. **Confusing and Inconsistent `getTopicPolicies` Methods**:
   - There are two overrides of `getTopicPolicies`:
     ```java
     TopicPolicies getTopicPolicies(TopicName topicName, boolean isGlobal) throws TopicPoliciesCacheNotInitException;
     TopicPolicies getTopicPolicies(TopicName topicName) throws TopicPoliciesCacheNotInitException;
     ```
   - The second method is equivalent to `getTopicPolicies(topicName, false)`.
   - These methods are asynchronous and start an asynchronous policies initialization, then try to get the policies from the cache. If the initialization hasn't started, they throw `TopicPoliciesCacheNotInitException`.

These methods are hard to use and are primarily used in tests. The `getTopicPoliciesAsyncWithRetry` method uses a user-provided executor and backoff policy to call `getTopicPolicies` until `TopicPoliciesCacheNotInitException` is not thrown:

```java
default CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsyncWithRetry(TopicName topicName,
          final Backoff backoff, ScheduledExecutorService scheduledExecutorService, boolean isGlobal) {
```

The `getTopicPolicies` methods are confusing for users who want to implement their own topic policies service. They need to look deeply into Pulsar's source code to understand these details.

[PR #21231](https://github.com/apache/pulsar/pull/21231) adds two asynchronous overrides that are more user-friendly:

```java
CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName, boolean isGlobal);
CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(@Nonnull TopicName topicName);
```

Now there are five asynchronous `get` methods. Unlike `getTopicPolicies`, `getTopicPoliciesAsync(topic)` is not equivalent to `getTopicPoliciesAsync(topic, false)`. Instead:
- `getTopicPoliciesAsync(topic)` tries getting local policies first, then global policies if absent.
- `getTopicPoliciesAsync(topic, true)` tries getting global policies.
- `getTopicPoliciesAsync(topic, false)` tries getting local policies.

Since [PR #12517](https://github.com/apache/pulsar/pull/12517), topic policies support global policies across clusters. Therefore, there are local and global policies.

Currently:
- `getTopicPoliciesAsync(TopicName)` is used in `BrokerService#getTopicPoliciesBypassSystemTopic` for initializing topic policies of `PersistentTopic` objects.
- `getTopicPoliciesAsyncWithRetry` is used in `AdminResource#getTopicPoliciesAsyncWithRetry` for all topic policies admin APIs.
- Other methods are used only in tests.

There is also a sixth method, `getTopicPoliciesIfExists`, which tries to get local topic policies from the cache:

```java
TopicPolicies getTopicPoliciesIfExists(TopicName topicName);
```

However, this method is called just because there was no `getTopicPoliciesAsync` methods before and `getTopicPolicies` is hard to use. For example, here is an example code snippet in `PersistentTopicsBase#internalUpdatePartitionedTopicAsync`:

```java
TopicPolicies topicPolicies =
        pulsarService.getTopicPoliciesService().getTopicPoliciesIfExists(topicName);
if (topicPolicies != null && topicPolicies.getReplicationClusters() != null) {
    replicationClusters = topicPolicies.getReplicationClustersSet();
}
```

With the new `getTopicPoliciesAsync` methods, this code can be replaced with:

```java
pulsarService.getTopicPoliciesService().getTopicPoliciesAsync(topicName, GetType.LOCAL_ONLY)
    .thenAccept(topicPolicies -> {
        if (topicPolicies.isPresent() && topicPolicies.get().getReplicationClusters() != null) {
            replicationClusters = topicPolicies.get().getReplicationClustersSet();
        }
    });
```

## Motivation

Make `TopicPoliciesService` pluggable so users can customize the topic policies service via another backend metadata store.

## Goals

### In Scope

Redesign a clear and simple `TopicPoliciesService` interface for users to customize.

## High-Level Design

Add a `topicPoliciesServiceClassName` configuration to specify the topic policies service class name. If the class name is not the default `SystemTopicBasedTopicPoliciesService`, `systemTopicEnabled` will not be required unless the implementation requires it.

## Detailed Design

### Design & Implementation Details

1. Add a unified method to get topic policies:
   ```java
   enum GetType {
       DEFAULT, // try getting the local topic policies, if not present, then get the global policies
       GLOBAL_ONLY, // only get the global policies
       LOCAL_ONLY,  // only get the local policies
   }
   CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type);
   ```

   `getTopicPoliciesAsyncWithRetry` will be replaced by `getTopicPoliciesAsync(topicName, LOCAL_ONLY)` or `getTopicPoliciesAsync(topicName, GLOBAL_ONLY)`. The other two original `getTopicPoliciesAsync` methods and `getTopicPoliciesIfExists` will be replaced by `getTopicPoliciesAsync(topicName, DEFAULT)`.

2. Move `addOwnedNamespaceBundleAsync` and `removeOwnedNamespaceBundleAsync` to private methods of `SystemTopicBasedTopicPoliciesService`.

3. Add a `TestUtils` class in tests to include `getTopicPolicies` and `getTopicPoliciesBypassCacheAsync` methods.

4. Remove the generic parameter from `TopicPolicyListener` as the value type should always be `TopicPolicies`. Mark this listener interface as `Stable`.

5. Add a `PulsarService` parameter to the `start` method so that the implementation can have a constructor with an empty parameter list and get the `PulsarService` instance from the `start` method.

6. Add a `boolean` return value to `registerListener` since `PersistentTopic#initTopicPolicy` checks if the topic policies are enabled. The return value will indicate if the `TopicPoliciesService` instance is `topicPoliciesServiceClassName.DISABLED`.

Since the topic policies service is now decoupled from system topics, remove all `isSystemTopicAndTopicLevelPoliciesEnabled()` calls.

Here is the refactored `TopicPoliciesService` interface:

```java
    /**
     * Delete policies for a topic asynchronously.
     *
     * @param topicName topic name
     */
    CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName);

    /**
     * Update policies for a topic asynchronously.
     *
     * @param topicName topic name
     * @param policies  policies for the topic name
     */
    CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, TopicPolicies policies);

    /**
     * It controls the behavior of {@link TopicPoliciesService#getTopicPoliciesAsync}.
     */
    enum GetType {
        DEFAULT, // try getting the local topic policies, if not present, then get the global policies
        GLOBAL_ONLY, // only get the global policies
        LOCAL_ONLY,  // only get the local policies
    }

    /**
     * Retrieve the topic policies.
     */
    CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type);

    /**
     * Start the topic policy service.
     */
    default void start(PulsarService pulsar) {
    }

    /**
     * Close the resources if necessary.
     */
    default void close() throws Exception {
    }

    /**
     * Registers a listener for topic policies updates.
     *
     * <p>
     * The listener will receive the latest topic policies when they are updated. If the policies are removed, the
     * listener will receive a null value. Note that not every update is guaranteed to trigger the listener. For
     * instance, if the policies change from A -> B -> null -> C in quick succession, only the final state (C) is
     * guaranteed to be received by the listener.
     * In summary, the listener is guaranteed to receive only the latest value.
     * </p>
     *
     * @return true if the listener is registered successfully
     */
    boolean registerListener(TopicName topicName, TopicPolicyListener listener);

    /**
     * Unregister the topic policies listener.
     */
    void unregisterListener(TopicName topicName, TopicPolicyListener listener);
```

```java
@InterfaceStability.Stable
public interface TopicPolicyListener {

  void onUpdate(TopicPolicies data);
}
```

### Configuration

Add a new configuration `topicPoliciesServiceClassName`.

## Backward & Forward Compatibility

If downstream applications need to call APIs from `TopicPoliciesService`, they should modify the code to use the new API.

## Alternatives

### Keep the `TopicPoliciesService` Interface Compatible

The current interface is poorly designed because it has only one implementation. Keeping these methods will burden developers who want to develop a customized interface. They need to understand where these confusing methods are called and handle them carefully.

## General Notes

## Links

* Mailing List discussion thread: https://lists.apache.org/thread/gf6h4n5n1z4n8v6bxdthct1n07onfdxt
* Mailing List voting thread: https://lists.apache.org/thread/potjbkb4w8brcwscgdwzlxnowgdf11gd
