---
id: functions-develop-api
title: Use APIs
sidebar_label: "Use APIs"
---

The following table outlines the APIs that you can use to develop Pulsar Functions in Java, Python, and Go.

| Interface | Description | Use case| 
|---------|------------|---------| 
| [Language-native interface for Java/Python](functions-develop-api-language-native) | No Pulsar-specific libraries or special dependencies required (only core libraries). | Functions that do not require access to the [context](functions-develop-context).| 
| [Pulsar Functions SDK for Java/Python/Go](functions-develop-api-sdk) | Pulsar-specific libraries that provide a range of functionality not available in the language-native interfaces,  such as state management or user configuration. | Functions that require access to the [context](functions-develop-context).| 
| [Extended Pulsar Functions SDK for Java](functions-develop-api-exended-sdk) | An extension to Pulsar-specific libraries, providing the initialization and close interfaces in Java. | Functions that require initializing and releasing external resources.| 