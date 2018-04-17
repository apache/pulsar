---
title: State storage for Pulsar Functions
preview: true
---

Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. All Pulsar installations, including local {% popover standalone %} installations, include a deployment of BookKeeper {% popover bookies %}.

## State storage API

The state storage API for Pulsar Functions is quite simple

Method | Description | Java | Python
:------|:------------|:-----|:------
Get value | Fetches the value associated with the specified key | `CompletableFuture<ByteBuffer> getValue(String key)` | `get_value`
Put value | Updates the value associated with a given key | `CompletableFuture<Void> putValue(String key, ByteBuffer value)`

### Serialization and deserialization

All values in the Pulsar Functions state storage API are raw byte arrays. *All serialization to and deserialization from other data types must be handled by the function*.

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

```python
from pulsar import Function

class StateFunction(Function):
    def process(self, input, context):
        value = context.get_value("some-key")
        if value is None:
            return "NOTHING"
        else:
            return str(value)
```