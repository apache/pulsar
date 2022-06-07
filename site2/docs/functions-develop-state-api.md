---
id: functions-develop-state-api
title: Call state APIs
sidebar_label: "Call state APIs"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Pulsar Functions expose APIs for mutating and accessing `state`. These APIs are available in the [Context](functions-develop-context.md) object when you use [Java/Python SDK](functions-develop-api-sdk.md) to develop functions.

The following table outlines the states that can be accessed within Java and Python functions.

| State-related API                       | API for Java                           | Python         |
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

## Retrieve counter

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

## Update state

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

## Retrieve state

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

## Delete state

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
