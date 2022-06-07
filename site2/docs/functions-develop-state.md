---
id: functions-develop-state
title: Configure state storage
sidebar_label: "Configure state storage"
---

Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. Pulsar integrates with BookKeeper [table service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f) to store state for functions. For example, a `WordCount` function can store the state of its counters into BookKeeper table service via [State APIs](functions-develop-state-api).

States are key-value pairs, where a key is a string and its value is arbitrary binary data - counters are stored as 64-bit big-endian binary values. Keys are scoped to an individual function, and shared between instances of that function.

:::note

State storage is **not** available for Go functions.

:::
