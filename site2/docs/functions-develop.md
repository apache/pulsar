---
id: functions-develop
title: Develop Pulsar Functions
sidebar_label: Develop functions
---

## Available APIs

In both Java and Python, you have two options to write Pulsar Functions.

Interface | Description | Use cases
:---------|:------------|:---------
Language-native interface | No Pulsar-specific libraries or special dependencies required (only core libraries from Java/Python) | Functions that don't require access to the function's [context](#context)
Pulsar Function SDK for Java/Python | Pulsar-specific libraries that provide a range of functionality not provided by "native" interfaces | Functions that require access to the function's [context](#context)

In Python, for example, this language-native function, which adds an exclamation point to all incoming strings and publishes the resulting string to a topic, would have no external dependencies:

```python
def process(input):
    return "{}!".format(input)
```

This function, however, would use the Pulsar Functions [SDK for Python](#python-sdk-functions):

```python
from pulsar import Function

class DisplayFunctionName(Function):
    def process(self, input, context):
        function_name = context.function_name()
        return "The function processing this message has the name {0}".format(function_name)
```
