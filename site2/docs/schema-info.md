---
id: schema-info
title: Schema info
sidebar_label: Schema info
---

Pulsar schema is defined in a data structure called `SchemaInfo`. 

The `SchemaInfo` is stored and enforced on a per-topic basis and cannot be stored at the namespace or tenant level.

A `SchemaInfo` consists of the following fields:

| Field | Description |
|---|---|
| `name` | Schema name (a string). |
| `type` | Schema type, which determines how to interpret the schema data. |
| `schema` | Schema data, which is a sequence of 8-bit unsigned bytes and schema-type specific. |
| `properties` | A map of string key/value pairs,which is application-specific. |

**Example**

This is the `SchemaInfo` of a string.

```text
{
    “name”: “test-string-schema”,
    “type”: “STRING”,
    “schema”: “”,
    “properties”: {}
}
```