---
id: functions-develop-metrics
title: Use metrics to monitor functions
sidebar_label: "Use metrics to monitor functions"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

To ensure that running functions are healthy at any time, you can configure functions to publish arbitrary metrics to the `metrics` interface that can be queried. 

:::note

Using the language-native interface for Java or Python is **not** able to publish metrics and stats to Pulsar.

:::

You can use both built-in metrics and customized metrics to monitor functions.
- Use the built-in [function metrics](reference-metrics.md#pulsar-functions).
  Pulsar Functions expose the metrics that can be collected and used for monitoring the health of Java, Python, and Go functions. You can check the metrics by following the [monitoring](deploy-monitoring.md/#function-and-connector-stats) guide. 
- Set your customized metrics.
  In addition to the built-in metrics, Pulsar allows you to customize metrics for Java and Python functions. Function workers collect user-defined metrics to Prometheus automatically and you can check them in Grafana.

Here is an example of how to customize metrics for Java, Python and Go functions by using the [`Context object`](functions-concepts.md#context) on a per-key basis. For example, you can set a metric for the `process-count` key and set another one for the `elevens-count` key every time the function processes a message. 


````mdx-code-block
<Tabs groupId="lang-choice"
  defaultValue="Java"
  values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"Go","value":"Go"}]}>
<TabItem value="Java">

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class MetricRecorderFunction implements Function<Integer, Void> {
    @Override
    public void apply(Integer input, Context context) {
        // Records the metric 1 every time a message arrives
        context.recordMetric("hit-count", 1);

        // Records the metric only if the arriving number equals 11
        if (input == 11) {
            context.recordMetric("elevens-count", 1);
        }

        return null;
    }
}

```

</TabItem>
<TabItem value="Python">

```python

from pulsar import Function

class MetricRecorderFunction(Function):
    def process(self, input, context):
        context.record_metric('hit-count', 1)

        if input == 11:
            context.record_metric('elevens-count', 1)

```

</TabItem>
<TabItem value="Go">

```go

func metricRecorderFunction(ctx context.Context, in []byte) error {
	inputstr := string(in)
	fctx, ok := pf.FromContext(ctx)
	if !ok {
		return errors.New("get Go Functions Context error")
	}
	fctx.RecordMetric("hit-count", 1)
	if inputstr == "eleven" {
		fctx.RecordMetric("elevens-count", 1)
	}
	return nil
}

```

</TabItem>
</Tabs>
````
