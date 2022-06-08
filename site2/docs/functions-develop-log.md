---
id: functions-develop-log
title: Produce function logs
sidebar_label: "Produce function logs"
---

## Produce logs for Java functions

Pulsar Functions that use the Java SDK have access to an [SLF4j `Logger`](https://www.slf4j.org/api/org/apache/log4j/Logger.html) object. The logger object can be used to produce logs at a specified log level. 

For example, the following function logs either a `WARNING`- or `INFO`-level log based on whether the incoming string contains the word `danger`.

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

To enable your function to produce logs, you need to specify a log topic when creating or running the function. The following is an example.

```bash

bin/pulsar-admin functions create \
  --jar my-functions.jar \
  --classname my.package.LoggingFunction \
  --log-topic persistent://public/default/logging-function-logs \
  # Other function configs

```

You can access all the logs produced by `LoggingFunction` via the `persistent://public/default/logging-function-logs` topic.

### Customize log levels for Java functions

By default, the log level for Java functions is `info`. If you want to customize the log level of your Java functions, for example, change it to `debug`, you can update the [`functions_log4j2.xml`](https://github.com/apache/pulsar/blob/master/conf/functions_log4j2.xml) file.

:::tip

The `functions_log4j2.xml` file is under your Pulsar configuration directory, for example, `/etc/pulsar/` on bare-metal, or `/pulsar/conf` on Kubernetes. 

:::

1. Set the value of `property`.

   ```xml

        <Property>
            <name>pulsar.log.level</name>
            <value>debug</value>
        </Property>

   ```

2. Apply the log level to places where they are referenced. In the following example, `debug` applies to all function logs. 

   ```xml

        <Root>
            <level>${sys:pulsar.log.level}</level>
            <AppenderRef>
                <ref>${sys:pulsar.log.appender}</ref>
                <level>${sys:pulsar.log.level}</level>
            </AppenderRef>
        </Root>

   ```

   To be more selective, you can apply different log levels to different classes or modules. For example:

   ```xml

        <Logger>
            <name>com.example.module</name>
            <level>info</level>
            <additivity>false</additivity>
            <AppenderRef>
                <ref>${sys:pulsar.log.appender}</ref>
            </AppenderRef>
        </Logger>

   ```

   To apply a more verbose log level to a class in the module, you can reference the following example:

   ```xml

        <Logger>
            <name>com.example.module.className</name>
            <level>debug</level>
            <additivity>false</additivity>
            <AppenderRef>
                <ref>Console</ref>
            </AppenderRef>
        </Logger>

   ```

   * `additivity` indicates whether log messages will be duplicated if multiple `<Logger>` entries overlap. Disabling additivity (`false`) prevents duplication of log messages when one or more `<Logger>` entries contain classes or modules that overlap.
   * `AppenderRef` allows you to output the log to a target specified in the definition of the `Appender` section. For example:

   ```xml

      <Console>
        <name>Console</name>
        <target>SYSTEM_OUT</target>
        <PatternLayout>
          <Pattern>%d{ISO8601_OFFSET_DATE_TIME_HHMM} [%t] %-5level %logger{36} - %msg%n</Pattern>
        </PatternLayout>
      </Console>

   ```

## Produce logs for Python functions

Pulsar Functions that use the Python SDK have access to a logger object. The logger object can be used to produce logs at a specified log level. 

For example, the following function logs either a `WARNING`- or `INFO`-level log based on whether the incoming string contains the word `danger`.

```python

from pulsar import Function

class LoggingFunction(Function):
    def process(self, input, context):
        logger = context.get_logger()
        msg_id = context.get_message_id()
        if 'danger' in input:
            logger.warn("A warning was received in message {0}".format(context.get_message_id()))
        else:
            logger.info("Message {0} received\nContent: {1}".format(msg_id, input))

```

To enable your function to produce logs, you need to specify a log topic when creating or running the function. The following is an example.

```bash

bin/pulsar-admin functions create \
  --py logging_function.py \
  --classname logging_function.LoggingFunction \
  --log-topic logging-function-logs \
  # Other function configs

```

All logs produced by `LoggingFunction` can be accessed via the `logging-function-logs` topic. Additionally, you can specify the function log levels through `context.get_logger().setLevel(level)`. For more information, refer to [Logging facility for Python](https://docs.python.org/3/library/logging.html#logging.Logger.setLevel) .

## Produce logs for Go functions

When you use `logTopic` related functionalities in Go functions, you can import `github.com/apache/pulsar/pulsar-function-go/logutil` rather than using the `getLogger()` context object.

The following function shows different log levels based on the function input.

```go
import (
    "context"

    "github.com/apache/pulsar/pulsar-function-go/pf"

    log "github.com/apache/pulsar/pulsar-function-go/logutil"
)

func loggerFunc(ctx context.Context, input []byte) {
	if len(input) <= 100 {
		log.Infof("This input has a length of: %d", len(input))
	} else {
		log.Warnf("This input is getting too long! It has {%d} characters", len(input))
	}
}

func main() {
	pf.Start(loggerFunc)
}

```
