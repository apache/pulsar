---
id: functions-develop-api-exended-sdk
title: Use extended SDK for Java
sidebar_label: "Use extended SDK for Java"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

This extended Pulsar Functions SDK provides two additional interfaces to initialize and release external resources.
- By using the `initialize` interface, you can initialize external resources which only need one-time initialization when the function instance starts.
- By using the `close` interface, you can close the referenced external resources when the function instance closes. 

:::note

The extended Pulsar Functions SDK for Java is only available in Pulsar 2.10.0 or later versions. Before using it, you need to [set up function workers](functions-worker.md) in Pulsar 2.10.0 or later versions.

:::

The following example uses the extended interface of Pulsar Functions SDK for Java to initialize RedisClient when the function instance starts and release it when the function instance closes.

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"}]}>
<TabItem value="Java">

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import io.lettuce.core.RedisClient;

public class InitializableFunction implements Function<String, String> {
    private RedisClient redisClient;
    
    private void initRedisClient(Map<String, Object> connectInfo) {
        redisClient = RedisClient.create(connectInfo.get("redisURI"));
    }

    @Override
    public void initialize(Context context) {
        Map<String, Object> connectInfo = context.getUserConfigMap();
        redisClient = initRedisClient(connectInfo);
    }
    
    @Override
    public String process(String input, Context context) {
        String value = client.get(key);
        return String.format("%s-%s", input, value);
    }

    @Override
    public void close() {
        redisClient.close();
    }
}

```

</TabItem>
</Tabs>
````
