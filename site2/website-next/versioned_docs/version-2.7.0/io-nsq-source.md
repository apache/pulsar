---
id: io-nsq-source
title: NSQ source connector
sidebar_label: "NSQ source connector"
original_id: io-nsq-source
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


The NSQ source connector receives messages from NSQ topics 
and writes messages to Pulsar topics.

## Configuration

The configuration of the NSQ source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `lookupds` |String| true | " " (empty string) | A comma-separated list of nsqlookupds to connect to. |
| `topic` | String|true | " " (empty string) | The NSQ topic to transport. |
| `channel` | String |false | pulsar-transport-{$topic} | The channel to consume from on the provided NSQ topic. |