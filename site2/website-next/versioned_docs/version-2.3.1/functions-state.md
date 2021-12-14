---
id: functions-state
title: Pulsar Functions State Storage (Developer Preview)
sidebar_label: "State Storage"
original_id: functions-state
---

Since Pulsar 2.1.0 release, Pulsar integrates with Apache BookKeeper [table service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f)
for storing the `State` for functions. For example, A `WordCount` function can store its `counters` state into BookKeeper's table service via Pulsar Functions [State API](#api).

## API

### Java API

Currently Pulsar Functions expose following APIs for mutating and accessing State. These APIs are available in the [Context](functions-api.md#context) object when
you are using [Java SDK](functions-api.md#java-sdk-functions) functions.

#### incrCounter

```java

    /**
     * Increment the builtin distributed counter referred by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

```

The application can use `incrCounter` to change the counter of a given `key` by the given `amount`.

#### incrCounterAsync

```java

     /**
     * Increment the builtin distributed counter referred by key
     * but dont wait for the completion of the increment operation
     *
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    CompletableFuture<Void> incrCounterAsync(String key, long amount);

```

The application can use `incrCounterAsync` to asynchronously change the counter of a given `key` by the given `amount`.

#### getCounter

```java

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

```

The application can use `getCounter` to retrieve the counter of a given `key` mutated by `incrCounter`.

Besides the `counter` API, Pulsar also exposes a general key/value API for functions to store
general key/value state.

#### getCounterAsync

```java

     /**
     * Retrieve the counter value for the key, but don't wait
     * for the operation to be completed
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    CompletableFuture<Long> getCounterAsync(String key);

```

The application can use `getCounterAsync` to asynchronously retrieve the counter of a given `key` mutated by `incrCounterAsync`.

#### putState

```java

    /**
     * Update the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

```

#### putStateAsync

```java

    /**
     * Update the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @param value state value of the key
     */
    CompletableFuture<Void> putStateAsync(String key, ByteBuffer value);

```

The application can use `putStateAsync` to asynchronously update the state of a given `key`.

#### getState

```

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);

```

#### getStateAsync

```java

    /**
     * Retrieve the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    CompletableFuture<ByteBuffer> getStateAsync(String key);

```

The application can use `getStateAsync` to asynchronously retrieve the state of a given `key`.

### Python API

State currently is not supported at [Python SDK](functions-api.md#python-sdk-functions).

## Query State

A Pulsar Function can use the [State API](#api) for storing state into Pulsar's state storage
and retrieving state back from Pulsar's state storage. Additionally Pulsar also provides
CLI commands for querying its state.

```shell

$ bin/pulsar-admin functions querystate \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <function-name> \
    --state-storage-url <bookkeeper-service-url> \
    --key <state-key> \
    [---watch]

```

If `--watch` is specified, the CLI will watch the value of the provided `state-key`.

## Example

### Java Example

{@inject: github:WordCountFunction:/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/WordCountFunction.java} is a very good example
demonstrating on how Application can easily store `state` in Pulsar Functions.

```java

public class WordCountFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) throws Exception {
        Arrays.asList(input.split("\\.")).forEach(word -> context.incrCounter(word, 1));
        return null;
    }
}

```

The logic of this `WordCount` function is pretty simple and straightforward:

1. The function first splits the received `String` into multiple words using regex `\\.`.
2. For each `word`, the function increments the corresponding `counter` by 1 (via `incrCounter(key, amount)`).

### Python Example

State currently is not supported at [Python SDK](functions-api.md#python-sdk-functions).

