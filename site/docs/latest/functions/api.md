---
title: The Pulsar Functions API
---

## Java

Java API example:

```java
import java.util.Function;
public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) { return String.format("%s!", input); }
}
```

### With context

```java
public interface PulsarFunction<I, O> {
    O process(I input, Context context) throws Exception;
}
```

Context interface:

```java
public interface Context {
    byte[] getMessageId();
    String getTopicName();
    Collection<String> getSourceTopics();
    String getSinkTopic();
    String getOutputSerdeClassName();
    String getTenant();
    String getNamespace();
    String getFunctionName();
    String getFunctionId();
    String getInstanceId();
    String getFunctionVersion();
    Logger getLogger();
    void incrCounter(String key, long amount);
    String getUserConfigValue(String key);
    void recordMetric(String metricName, double value);
    <O> CompletableFuture<Void> publish(String topicName, O object, String serDeClassName);
    <O> CompletableFuture<Void> publish(String topicName, O object);
    CompletableFuture<Void> ack(byte[] messageId, String topic);
}
```

### SerDe

> Serde stands for **Ser**ialization and **De**serialization.

Built-in vs. custom. For custom, you need to implement this interface:

```java
public interface SerDe<T> {
    T deserialize(byte[] input);
    byte[] serialize(T input);
}
```

The built-in is the `org.apache.pulsar.functions.api.DefaultSerDe` class:

```java

```

The built-in should work fine for basic Java types. For more advanced types,


## Python

```python
def process(input):
```