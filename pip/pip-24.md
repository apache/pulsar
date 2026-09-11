# PIP-24: Simplify memory settings

* **Status**: Accepted
* **Author**: [Matteo Merli](https://github.com/merlimat)
* **Pull Request**:
* **Mailing List discussion**:
* **Release**: 2.3.0


## Motivation

Configuring the correct JVM memory settings and cache sizes for a Pulsar cluster should be
simplified.

There are currently many knobs in Netty or JVM flags for different components and while
with a good setup the systems is very stable, it's easy to setup non-optimal configurations
which might result in OutOfMemory errors under load.

Ideally, there should be very minimal configuration required to bring up a Pulsar cluster
that can work under a wide set of traffic loads. In any case, we should prefer to automatically
fallback to slower alternatives, when possible, instead of throwing OOM exceptions.

## Goals

 1. Default setting should allow Pulsar to use the all the memory as configured on the JVM,
    irrespective of Direct vs Heap memory
 1. Automatically set the size of caches based on the amount of memory available to the JVM
 1. Allow to disable pooling completely for environments where memory is scarce
 1. Allow to configure different policies to have different fallback options when the memory
    quotas are reached


## Changes

### Netty Allocator Wrapper

Create an allocator wrapper that can be configured with different behaviors. This will be
using the regular `PooledByteBufAllocator` but will have a configuration object to decide
what to do in particular moments. It will also serve as a way to group and simplify all
the Netty allocator options which are currently spread across multiple system properties,
for which the documentation is not easily searchable.

The wrapper will be configured and instantianted through a builder class:

```java
public interface ByteBufAllocatorBuilder {

    /**
     * Creates a new {@link ByteBufAllocatorBuilder}.
     */
    public static ByteBufAllocatorBuilder create() {
        return new ByteBufAllocatorBuilderImpl();
    }

    /**
     * Finalize the configured {@link ByteBufAllocator}
     */
    ByteBufAllocator build();

    /**
     * Specify a custom allocator where the allocation requests should be forwarded to.
     *
     * <p>
     * Default is to used {@link PooledByteBufAllocator#DEFAULT} when pooling is required or
     * {@link UnpooledByteBufAllocator} otherwise.
     */
    ByteBufAllocatorBuilder allocator(ByteBufAllocator allocator);

    /**
     * Define the memory pooling policy
     *
     * <p>
     * Default is {@link PoolingPolicy#PooledDirect}
     */
    ByteBufAllocatorBuilder poolingPolicy(PoolingPolicy policy);

    /**
     * Controls the amount of concurrency for the memory pool.
     *
     * <p>
     * Default is to have a number of allocator arenas equals to 2 * CPUS.
     * <p>
     * Decreasing this number will reduce the amount of memory overhead, at the expense of increased allocation
     * contention.
     */
    ByteBufAllocatorBuilder poolingConcurrency(int poolingConcurrency);

    /**
     * Define the OutOfMemory handling policy
     *
     * <p>
     * Default is {@link OomPolicy#FallbackToHeap}
     */
    ByteBufAllocatorBuilder oomPolicy(OomPolicy policy);

    /**
     * Enable the leak detection for
     *
     * <p>
     * Default is {@link LeakDetectionPolicy#Disabled}
     */
    ByteBufAllocatorBuilder leakDetectionPolicy(LeakDetectionPolicy leakDetectionPolicy);
}
```

The policies are here defined:

```java
/**
 * Define a policy for allocating buffers
 */
public enum PoolingPolicy {

    /**
     * Allocate memory from JVM heap without any pooling.
     *
     * This option has the least overhead in terms of memory usage since the memory will be automatically reclaimed by
     * the JVM GC but might impose a performance penalty at high throughput.
     */
    UnpooledHeap,

    /**
     * Use Direct memory for all buffers and pool the memory.
     *
     * Direct memory will avoid the overhead of JVM GC and most memory copies when reading and writing to socket
     * channel.
     *
     * Pooling will add memory space overhead due to the fact that there will be fragmentation in the allocator and that
     * threads will keep a portion of memory as thread-local to avoid contention when possible.
     */
    PooledDirect
}

/**
 * Represents the action to take when it's not possible to allocate memory.
 */
public enum OomPolicy {

    /**
     * Throw regular OOM exception without taking addition actions
     */
    ThrowException,

    /**
     * If it's not possible to allocate a buffer from direct memory, fallback to allocate an unpooled buffer from JVM
     * heap.
     *
     * This will help absorb memory allocation spikes because the heap allocations will naturally slow down the process
     * and will result if full GC cleanup if the Heap itself is full.
     */
    FallbackToHeap,

    /**
     * If it's not possible to allocate memory, kill the JVM process so that it can be restarted immediately.
     *
     */
    KillProcess,
}

/**
 * Define the policy for the Netty leak detector
 */
public enum LeakDetectionPolicy {

    /**
     * No leak detection and no overhead
     */
    Disabled,

    /**
     * Instruments 1% of the allocated buffer to track for leaks
     */
    Simple,

    /**
     * Instruments 1% of the allocated buffer to track for leaks, reporting stack traces of places where the buffer was
     * used
     */
    Advanced,

    /**
     * Instruments 100% of the allocated buffer to track for leaks, reporting stack traces of places where the buffer
     * was used. Introduce very significant overhead.
     */
    Paranoid,
}
```

It will be possible to create an allocator through the builder and then pass it through
to Netty client/server or just directly allocate buffers.

```java
ByteBufAllocator allocator = ByteBufAllocatorBuilder.create()
        .poolingPolicy(PoolingPolicy.PooledDirect)
        .oomPolicy(OomPolicy.FallbackToHeap)
        .leakDetectionPolicy(LeakDetectionPolicy.Disabled)
        .build();
```

### Component changes

In addition to used the policies based allocator wrapper, each component will have
additional changes.

#### Pulsar broker

Add configuration options in `broker.conf` to allow configuration of the allocator. Eg.:

```properties
allocatorPoolingPolicy=PooledDirect
allocatorPoolingConcurrency=4
allocatorOomPolicy=FallbackToHeap
allocatorLeakDetectionPolicy=Disabled
```

##### Managed ledger cache

Currently, in Pulsar broker, the only memory pooled from the direct memory region, in
addition to regular IO buffer is the ManagedLedgerCache. This cache is used to dispatch
directly to consumers (once a message is persisted), avoiding reads from bookies for
consumers that are caught up with producers.

By default, the managed ledger cache size will be set to 1/3rd of the total available
direct memory (or heap if pooling is disabled).

The setting will be left empty to indicate the default dynamic behavior:

```
managedLedgerCacheSizeMb=
```

#### BookKeeper Client

Add options to configure the allocator in `ClientConfiguration` object.

#### Bookie

Add options to configure the allocator in `ServerConfiguration` object and `bookkeeper.conf`.

By default, in Pulsar we configure BookKeeper to use DbLedgerStorage. This storage
implementation has 2 main sources of memory allocations, the read and write caches.

By default, the configured direct memory region will be divided into 3 portions:
 * IO buffers - (50% and max to 4GB)
 * Write cache - 25 %
 * Read cache - 25 %

If there is a lot of direct memory available, max 4GB will be assigned to IO buffers and
the rest will be split between read and write caches.

This will still not take into account the memory used by RocksDB block cache, since this
will be allocated from within the JNI library and not accounted for in JVM heap or
direct memory regions.

The rule of thumb here would be to default to a size pegged to the direct memory size,
say 1/5th of it.

#### Pulsar Client

Add options to configure allocator policies in `PulsarClientBuilder`.

Additionally, for `PulsarClient` we should be able to define a max amount of memory
that a single instance is allowed to use.

This memory will be used when accumulating messages in the producers pending messages
queue or consumer receiving queues.

When the assigned client memory is filled up, some actions will be taken:

 * For producer it would be the same as the producer queue full condition, with either
   immediate send error or blocking behavior, depending on existing configuration.
 * For consumers, the flow control mechanism will be slowed down, by not asking the
   brokers for more messages, once the memory is full.

A reasonable default might be to use 64 MB per client instance, which will be shared
across all producers consumers created by that instance.
