---
id: functions-develop-state-example
title: Example
sidebar_label: "Example"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

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
