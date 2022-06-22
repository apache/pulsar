---
id: functions-develop-user-defined-configs
title: Pass user-defined configurations
sidebar_label: "Pass user-defined configurations"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

When you run or update functions created via SDK, you can pass arbitrary key/value pairs to them by using CLI with the `--user-config` flag. Key/value pairs must be specified as JSON. 

````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>
<TabItem value="Java">

:::note

For all key/value pairs passed to Java functions, both keys and values are `string`. To set the value to be a different type, you need to deserialize it from the `string` type.

:::

The context object of Java SDK enables you to access key/value pairs provided to Pulsar Functions via CLI (as JSON). The following example passes a key/value pair.

```bash

bin/pulsar-admin functions create \
  # Other function configs
  --user-config '{"word-of-the-day":"verdure"}'

```

To access that value in a Java function:

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.Optional;

public class UserConfigFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        Logger LOG = context.getLogger();
        Optional<String> wotd = context.getUserConfigValue("word-of-the-day");
        if (wotd.isPresent()) {
            LOG.info("The word of the day is {}", wotd);
        } else {
            LOG.warn("No word of the day provided");
        }
        return null;
    }
}

```

The `UserConfigFunction` function logs the string `"The word of the day is verdure"` every time the function is invoked. The `word-of-the-day` config can be changed only when the function is updated with a new value via the CLI.

You can also access the entire user config map or set a default value in case no value is present.

```java

// Get the whole config map
Map<String, String> allConfigs = context.getUserConfigMap();

// Get value or resort to default
String wotd = context.getUserConfigValueOrDefault("word-of-the-day", "perspicacious");

```

</TabItem>
<TabItem value="Python">

In a Python function, you can access the configuration value like this.

```python

from pulsar import Function

class WordFilter(Function):
    def process(self, context, input):
        forbidden_word = context.user_config()["forbidden-word"]

        # Don't publish the message if it contains the user-supplied
        # forbidden word
        if forbidden_word in input:
            pass
        # Otherwise publish the message
        else:
            return input

```

The context object of Python SDK enables you to access key/value pairs provided to functions via the command line (as JSON). The following example passes a key/value pair.

```bash

bin/pulsar-admin functions create \
  # Other function configs \
  --user-config '{"word-of-the-day":"verdure"}'

```

To access that value in a Python function:

```python

from pulsar import Function

class UserConfigFunction(Function):
    def process(self, input, context):
        logger = context.get_logger()
        wotd = context.get_user_config_value('word-of-the-day')
        if wotd is None:
            logger.warn('No word of the day provided')
        else:
            logger.info("The word of the day is {0}".format(wotd))

```

</TabItem>
<TabItem value="Go">

The context object of Go SDK enables you to access key/value pairs provided to functions via the command line (as JSON). The following example passes a key/value pair.

```bash

bin/pulsar-admin functions create \
  --go path/to/go/binary
  --user-config '{"word-of-the-day":"lackadaisical"}'

```

To access that value in a Go function:

```go

func contextFunc(ctx context.Context) {
  fc, ok := pf.FromContext(ctx)
  if !ok {
    logutil.Fatal("Function context is not defined")
  }

  wotd := fc.GetUserConfValue("word-of-the-day")

  if wotd == nil {
    logutil.Warn("The word of the day is empty")
  } else {
    logutil.Infof("The word of the day is %s", wotd.(string))
  }
}

```

</TabItem>
</Tabs>
````
