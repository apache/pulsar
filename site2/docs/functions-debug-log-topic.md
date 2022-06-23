---
id: functions-debug-log-topic
title: Debug with logic topic
sidebar_label: "Debug with logic topic"
---

When using Pulsar Functions, you can generate logs predefined in functions to a specified log topic and configure consumers to consume messages from the log topic. 

For example, the following function logs either a WARNING-level or INFO-level log based on whether the incoming string contains the word `danger` or not.

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class LoggingFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        Logger LOG = context.getLogger();
        String messageId = new String(context.getMessageId());

        if (input.contains("danger")) {
            LOG.warn("A warning was received in message {}", messageId);
        } else {
            LOG.info("Message {} received\nContent: {}", messageId, input);
        }

        return null;
    }
}

```

As shown in the example, you can get the logger via `context.getLogger()` and assign the logger to the `LOG` variable of `slf4j`, so you can define your desired logs in a function using the `LOG` variable. 

Meanwhile, you need to specify the topic that the logs can be produced to. The following is an example.

```bash

bin/pulsar-admin functions create \
  --log-topic persistent://public/default/logging-function-logs \
  # Other function configs

```

The message published to a log topic contains several properties: 
- `loglevel`: the level of the log message.
- `fqn`: the fully qualified function name that pushes this log message.
- `instance`: the ID of the function instance that pushes this log message.
