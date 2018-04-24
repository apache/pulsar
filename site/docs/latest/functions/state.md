---
title: State storage for Pulsar Functions
preview: true
---

Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. All Pulsar installations, including local {% popover standalone %} installations, include a deployment of BookKeeper {% popover bookies %}---thus, state storage comes "out of the box" for Pulsar Functions.

## API

The state storage API for Pulsar Functions is very simple and consists of just a handful of operations:

Operation | Description
:---------|:------------
Get value | Fetches the value associated with the specified key (if any)
Put value | Updates the value associated with a given key
Increment counter | Increments a specific counter specified by key (you can also decrement using negative increments)
Get counter | Fetches the current value associated with a counter (or zero if the counter has never been used)

## Deployment

{% include admonition.html type="success" content="For most Pulsar installations, you won't need to specify an alternative state storage service URL. If you're just using Pulsar's built-in Apache BookKeeper storage system, Pulsar Functions running in both [local run](../deployment#local-run) and [cluster mode](../deployment#cluster-mode) can use state storage." %}

## Java

The state storage API for Java is provided by the {% javadoc Context client org.apache.pulsar.functions.api.Context %} class. Here's an example usage of the context object for state storage and retrieval:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class StateFunction implements Function<String, String> {
    private static final String VALUE = "some value";

    public String process(String key, Context context) {
        Logger LOG = context.getLogger();

        // Fetch the value associated with the same key
        CompletableFuture<Optional<ByteBuffer>> futureValue = context.getValue(key);

        // Extract the fetched value and return it as the function's output
        futureValue.thenAccept(value -> {
            if (value.isPresent()) {
                String fetchedvalue = new String(value.get().array());
                return fetchedValue;
            } else {
                LOG.error("No value associated with the key {}", key);
                return null;
            }
        });
    }
}
```

The function above listens for incoming strings, uses those strings as state keys, and then seeks to fetch the state value associated with that key. If a value is present, the function returns that value, otherwise the function returns nothing (`null` in this case).

#### Java State storage API {#api}

The state storage API for Pulsar Functions is a fairly simple get/put-style key/value interface with just a few methods:

Operation | `Context` method
:---------|:----------------
Get value | `CompletableFuture<ByteBuffer> getValue(String key)`
Put value | `CompletableFuture<Void> putValue(String key, ByteBuffer value)`
Increment counter | `CompletableFuture<Void> incrCounter(String key, long increment)`
Get counter value | `CompletableFuture<Long> getCounter(String key)`

#### Data types

In the Pulsar Functions state storage API, all keys are strings and all values are raw byte arrays. *All serialization to and deserialization from other data types must be handled by the function*.