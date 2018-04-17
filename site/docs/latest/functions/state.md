---
title: State storage for Pulsar Functions
preview: true
---

Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. All Pulsar installations, including local {% popover standalone %} installations, include a deployment of BookKeeper {% popover bookies %}.

## Deployment

{% include admonition.html type="success" content="For most Pulsar installations, you won't need to specify an alternative state storage service URL. If you're just using Pulsar's built-in Apache BookKeeper storage system, Pulsar Functions running in both [local run](../deployment#local-run) and [cluster mode](../deployment#cluster-mode) can use state storage." %}

## Example uses

### Java

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class StateFunction implements Function<String, String> {
    private String fn(Optional<ByteBuffer> value) {
        return (value.isPresent()) ? value.toString() : "NOTHING";
    }

    public String process(String input, Context context) {
        CompletableFuture<Optional<ByteBuffer>> futureValue = context.getValue("some-key");

        futureValue.thenAccept(this::fn);
    }
}
```

### Python

```python
from pulsar import Function

class StateFunction(Function):
    def handle_value(value, input):
        if value is None:
            return input
        else:
            word = str(value)
            return "The word is {0}".format(word)

    def process(self, input, context):
        return handle_value(context.get_value("some-key"), input)
```

## State storage API

The state storage API for Pulsar Functions is a fairly simple get/put-style key/value interface with just a few methods:

Method | Description | Java | Python
:------|:------------|:-----|:------
Get value | Fetches the value associated with the specified key | `CompletableFuture<ByteBuffer> getValue(String key)` | `get_value(key)`
Put value | Updates the value associated with a given key | `CompletableFuture<Void> putValue(String key, ByteBuffer value)` | `put_value(key, value)`

### Data types

In the Pulsar Functions state storage API, all keys are strings and all values are raw byte arrays. *All serialization to and deserialization from other data types must be handled by the function*. Here's an example Java function that handles state storage values as strings:

```java
public class StringStateFunction implements Function<String, String> {
    @Override
    public String process(String input, Context context) {
        
    }
}
```