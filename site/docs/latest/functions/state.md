---
title: State storage for Pulsar Functions
preview: true
---

Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. All Pulsar installations, including local {% popover standalone %} installations, include a deployment of BookKeeper {% popover bookies %}.

```java
public class StateFunction implements Function<String, String> {
    public String process(String input, Context context) {
        
    }
}
```