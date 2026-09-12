# PIP-45: Pluggable metadata interface

* **Status**: Proposal
* **Author**: Matteo Merli
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:


## Goals

Provide a unified pluggable interface that can abstract all the Pulsar metadata
interactions.

After the refactoring, the default implementation will still be based on ZooKeeper and it will
be 100% compatible with the existing metadata. The metadata will be kept in the same location
and in the same exact format.

Once we have the interface defined we could have multiple backend implementations:
  * ZooKeeper
  * Etcd
  * In memory - for unit tests purposes
  * On local disk - for usage in Pulsar standalone

## Context

Pulsar is currently using ZooKeeper for metadata and coordination purposes. These accesses are
being done from Pulsar brokers and some administrative CLI tools. BookKeeper already supports
a pluggable metadata store.

Additionally, ZooKeeper client API is being accessed from several places in the codebase, so we
first need to consolidate all these accesses through a single generic `MetadataStore` interface.

This interface is based on the needs that Pulsar has in interacting with metadata and with the
semantics offered by existing metadata stores (eg. ZooKeeper, Etcd and others). The API will be
considered as "Beta" (meaning it could be evolved in breaking way) until we have at least few
concrete implementations. Therefore, at least initially, this will be an internal Pulsar API and
it will not be open to user plugins.

## Refactoring steps

### 1. Define metadata store API

The metadata store is modeled after a basic Key-Value interface with `compareAndSet()` updates
based on the version of a particular value.

```java
public interface MetadataStore extends AutoCloseable {

    /**
     * Read the value of one key, identified by the path
     *
     * The async call will return a future that yields a {@link GetResult} that will contain the value and the
     * associated {@link Stat} object.
     *
     * If the value is not found, the future will yield an empty {@link Optional}.
     *
     * @param path
     *            the path of the key to get from the store
     * @return a future to track the async request
     */
    CompletableFuture<Optional<GetResult>> get(String path);

    /**
     * Return all the nodes (lexicographically sorted) that are children to the specific path.
     *
     * If the path itself does not exist, it will return an empty list.
     *
     * @param path
     *            the path of the key to check on the store
     * @return a future to track the async request
     */
    CompletableFuture<List<String>> getChildren(String path);

    /**
     * Read whether a specific path exists.
     *
     * Note: In case of keys with multiple levels (eg: '/a/b/c'), checking the existence of a parent (eg. '/a') might
     * not necessarily return true, unless the key had been explicitly created.
     *
     * @param path
     *            the path of the key to check on the store
     * @return a future to track the async request
     */
    CompletableFuture<Boolean> exists(String path);

    /**
     * Put a new value for a given key.
     *
     * The caller can specify an expected version to be atomically checked against the current version of the stored
     * data.
     *
     * The future will return the {@link Stat} object associated with the newly inserted value.
     *
     *
     * @param path
     *            the path of the key to delete from the store
     * @param value
     *            the value to
     * @param expectedVersion
     *            if present, the version will have to match with the currently stored value for the operation to
     *            succeed. Use -1 to enforce a non-existing value.
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion);

    /**
     *
     * @param path
     *            the path of the key to delete from the store
     * @param expectedVersion
     *            if present, the version will have to match with the currently stored value for the operation to
     *            succeed
     * @throws NotFoundException
     *             if the path is not found
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion);
}
```

Additionally, when the `MetadataStore` is created, it should be possible to specify an observer
function that will be triggered whenever there are changes on a sub-tree of the specified keys.

This will be used to keep local caches updated without any polling.

### 2. Define coordination interface

Pulsar broker uses "coordination" in several different places. Examples are:

 * List of active brokers and their current load data report
 * Acquire ownership on a portion of a namespace topics (bundle)
 * Leader election (load manager)
 * Counters for generating unique prefix identifier

While in general these can be implemented through a Key-Value interface with the help of
flags (eg. "ephemeral" nodes in ZooKeeper), each backend system might have a more direct way
to implement these.

```java
/**
 * Interface for the coordination service. Provides abstraction for distributed locks and leader election.
 */
public interface CoordinationService extends AutoCloseable {

    /**
     * Read the content of an existing lock.
     *
     * If the lock is already taken, this operation will fail immediately.
     *
     * Warning: because of the distributed nature of the lock, having acquired a lock will never provide a strong
     * guarantee that no one else also think it owns the same resource. The caller will have to deal with these race
     * conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param path
     *            the path of the resource on which to acquire the lock
     * @param content
     *            the payload of the lock
     * @return a future that will track the completion of the operation
     * @throws NotFoundException
     *             if the lock is not taken
     * @throws CoordinationServiceException
     *             if there's a failure in reading the lock
     */
    CompletableFuture<Optional<byte[]>> readLock(String path);

    /**
     * Acquire a lock on a shared resource.
     *
     * If the lock is already taken, this operation will fail immediately.
     *
     * Warning: because of the distributed nature of the lock, having acquired a lock will never provide a strong
     * guarantee that no one else also think it owns the same resource. The caller will have to deal with these race
     * conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param path
     *            the path of the resource on which to acquire the lock
     * @param content
     *            the payload of the lock
     * @return a future that will track the completion of the operation
     * @throws ResourceBusyException
     *             if the lock is already taken
     * @throws CoordinationServiceException
     *             if there's a failure in acquiring the lock
     */
    CompletableFuture<ResourceLock> acquireLock(String path, byte[] content);

    /**
     * List all the locks that are children of a specific path.
     *
     * For example, given locks: <code>/a/b/lock-1</code> and <code>/a/b/lock-2</code>, the <code>listLocks()</code>
     * will return a list of <code>["lock-1", "lock-2"]</code>.
     *
     * @param path
     *            the prefix path to get the list of locks
     * @return a future that will track the completion of the operation
     * @throws CoordinationServiceException
     *             if there's a failure in getting the list of locks
     */
    CompletableFuture<List<String>> listLocks(String path);

    /**
     * Try to become the leader for the specified resource.
     *
     * If there's already a leader, this request will be kept pending the current process is the one to become the
     * leader.
     *
     *
     * Warning: because of the distributed nature of the leader election, having been promoted to "leader" status will
     * never provide a strong guarantee that no one else also thinks it's the leader. The caller will have to deal with
     * these race conditions when using the resource itself (eg. using compareAndSet() or fencing mechanisms).
     *
     * @param path
     *            the path of the resource of which to become the leader
     * @param content
     *            the payload of the lock
     * @return a future that will track the completion of the operation
     * @throws CoordinationServiceException
     *             if there's a failure in the leader election
     */
    CompletableFuture<ResourceLock> becomeLeader(String path, byte[] content);

    /**
     * Increment a counter identified by the specified path and return the current value.
     *
     * The counter value will be guaranteed to be unique within the context of the path.
     *
     * @param path
     *            the path that identifies a particular counter
     * @return a future that will track the completion of the operation
     * @throws CoordinationServiceException
     *             if there's a failure in incrementing the counter
     */
    CompletableFuture<Long> getNextCounterValue(String path);
}


/**
 * Represent a lock that the current process has on a shared resource.
 */
public interface ResourceLock {

    /**
     * @return the content associated with the lock
     */
    byte[] getContent();

    /**
     * Release the lock on the resource.
     *
     * @return a future to track when the release operation is complete
     */
    CompletableFuture<Void> release();

    /**
     * Get a future that can be used to get notified when the lock is not more valid.
     *
     * Note: the future will not be triggered when the lock is voluntarily released.
     *
     * @return a future to get notification if the lock is expired
     */
    CompletableFuture<Void> getLockExpiredFuture();
}
```

### 3. Port ManagedLedger to use MetadataStore

ManagedLedger is already using an abstraction for metadata access (see `MetaStore`). It will be
easy to convert that to the `MetadataStore` API.

### 4. Define metadata cache API

Currently, most metadata read accesses are happening through the `ZooKeeperCache` and additional
classes based on it, like `ZooKeeperDataCache` and `ZooKeeperChildrenCache`.

The cache needs to be ported to use `MetadataStore` API, along with the support for receiving
notifications and invalidating the stale entries.

An additional concept that should be added to the cache is the atomic "read-modify-update" operation.

This is currently being performed from many places in the code base and it should be consolidated
into a single implementation. For example:

```java
public interface TypedMetadataCache<T> {
    // ...

    /**
     * Perform an atomic read-modify-update of the value.
     *
     * The modify function can potentially be called multiple times if there are concurrent updates happening.
     *
     * @param path
     *            the path of the value
     * @param modifyFunction
     *            a function that will be passed the current value and returns a modified value to be stored
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> readModifyUpdate(String path, Function<T, T> modifyFunction);
}
```

### 5. Define higher level abstraction for metadata

While the ZooKeeperCache is already typed (in order to store the object already deserialized), we
should have an additional abstraction layer to mediate the access to the metadata.

For example, to get the list of tenants in a cluster, we shouldn't use `MetadataStore.getChildren()`
directly from multiple places. Rather, we need to provide a `ConfigurationStore` interface such as:

```java
public interface ConfigurationStore {
    CompletableFuture<List<String>> getTenants();

    CompletableFuture<List<String>> getNamespaces(String tenant);

    // ....
}
```
