---
id: functions-develop-state
title: Configure state storage
sidebar_label: "Configure state storage"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. Pulsar integrates with BookKeeper [table service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f) to store state for functions. For example, a `WordCount` function can store the state of its counters into BookKeeper table service via [State APIs](#call-state-apis).

States are key-value pairs, where a key is a string and its value is arbitrary binary data - counters are stored as 64-bit big-endian binary values. Keys are scoped to an individual function, and shared between instances of that function.

:::note

State storage is **not** available for Go functions.

:::


## Call state APIs

Pulsar Functions expose APIs for mutating and accessing `state`. These APIs are available in the [Context](functions-concepts.md#context) object when you use [Java/Python SDK](functions-develop-api.md) to develop functions.

The following table outlines the states that can be accessed within Java and Python functions.

| State-related API                       | Java                                   | Python         |
|-----------------------------------------|----------------------------------------|----------------|
| [Increment counter](#increment-counter) | `incrCounter` <br />`incrCounterAsync` | `incr_counter` |
| [Retrieve counter](#retrieve-counter)   | `getCounter` <br />`getCounterAsync`   | `get_counter`  |
| [Update state](#update-state)           | `putState` <br />`putStateAsync`       | `put_state`    |
| [Retrieve state](#retrieve-state)       | `getState` <br />`getStateAsync`       | `get_state`    |
| [Delete state](#delete-state)           | `deleteState`                          | `del_counter`  |


## Increment counter

You can use `incrCounter` to increment the counter of a given `key` by the given `amount`.
If the `key` does not exist, a new key is created.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

```java

    /**
     * Increment the builtin distributed counter referred by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

```

</TabItem>
<TabItem value="Python">

```python

  def incr_counter(self, key, amount):
    """incr the counter of a given key in the managed state"""

```

</TabItem>
</Tabs>
````

To asynchronously increment the counter, you can use `incrCounterAsync`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

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

</TabItem>
</Tabs>
````

### Retrieve counter

You can use `getCounter` to retrieve the counter of a given `key` mutated by `incrCounter`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

```java

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

```

</TabItem>
<TabItem value="Python">

```python

  def get_counter(self, key):
    """get the counter of a given key in the managed state"""

```

</TabItem>
</Tabs>
````

To asynchronously retrieve the counter mutated by `incrCounterAsync`, you can use `getCounterAsync`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

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

</TabItem>
</Tabs>
````

### Update state

Besides the `counter` API, Pulsar also exposes a general key/value API for functions to store and update the state of a given `key`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

```java

    /**
     * Update the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

```

</TabItem>
<TabItem value="Python">

```python

  def put_state(self, key, value):
    """update the value of a given key in the managed state"""

```

</TabItem>
</Tabs>
````

To asynchronously update the state of a given `key`, you can use `putStateAsync`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

```java

    /**
     * Update the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @param value state value of the key
     */
    CompletableFuture<Void> putStateAsync(String key, ByteBuffer value);

```

</TabItem>
</Tabs>
````

### Retrieve state

You can use `getState` to retrieve the state of a given `key`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">

```java

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);

```

</TabItem>
<TabItem value="Python">

```python

  def get_state(self, key):
    """get the value of a given key in the managed state"""

```

</TabItem>
</Tabs>
````

To asynchronously retrieve the state of a given `key`, you can use `getStateAsync`.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

```java

    /**
     * Retrieve the state value for the key, but don't wait for the operation to be completed
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    CompletableFuture<ByteBuffer> getStateAsync(String key);

```

</TabItem>
</Tabs>
````

### Delete state

:::note

Both counters and binary values share the same keyspace, so this API deletes either type.

:::

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

```java

    /**
     * Delete the state value for the key.
     *
     * @param key   name of the key
     */
    void deleteState(String key);

```

</TabItem>
</Tabs>
````


## Query state via CLI

Besides using the [State APIs](#call-state-apis) to store the state of functions in Pulsar's state storage and retrieve it back from the storage, you can use CLI commands to query the state of functions.

```bash

bin/pulsar-admin functions querystate \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <function-name> \
    --state-storage-url <bookkeeper-service-url> \
    --key <state-key> \
    [---watch]

```

If `--watch` is specified, the CLI tool keeps running to get the latest value of the provided `state-key`.


## Example

The example of `WordCountFunction` demonstrates how `state` is stored within Pulsar Functions.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"}]}>
<TabItem value="Java">


The logic of {@inject: github:`WordCountFunction`:/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/WordCountFunction.java} is simple and straightforward:

1. The function splits the received `String` into multiple words using regex `\\.`.
2. For each `word`, the function increments `counter` by 1 via `incrCounter(key, amount)`.

   ```java

   import org.apache.pulsar.functions.api.Context;
   import org.apache.pulsar.functions.api.Function;

   import java.util.Arrays;

   public class WordCountFunction implements Function<String, Void> {
       @Override
       public Void process(String input, Context context) throws Exception {
           Arrays.asList(input.split("\\.")).forEach(word -> context.incrCounter(word, 1));
           return null;
       }
   }

   ```

</TabItem>
<TabItem value="Python">

The logic of this `WordCount` function is simple and straightforward:

1. The function first splits the received string into multiple words.
2. For each `word`, the function increments `counter` by 1 via `incr_counter(key, amount)`.

   ```python

   from pulsar import Function

   class WordCount(Function):
       def process(self, item, context):
           for word in item.split():
               context.incr_counter(word, 1)

   ```

</TabItem>
</Tabs>
````
