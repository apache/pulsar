---
id: io-solr
title: solr Connector
sidebar_label: "solr Connector"
original_id: io-solr
---

## Sink

The solr Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a solr collection.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `solrUrl` | `null` | `true` | Comma separated zookeeper hosts with chroot used in SolrCloud mode (eg: localhost:2181,localhost:2182/chroot) or Url to connect to solr used in Standalone mode (e.g. localhost:8983/solr). |
| `solrMode` | `SolrCloud` | `true` | The client mode to use when interacting with the Solr cluster. Possible values [Standalone, SolrCloud]. |
| `solrCollection` | `null` | `true` | Solr collection name to which records need to be written. |
| `solrCommitWithinMs` | `10` | `false` | Commit within milli seconds for solr update, if none passes defaults to 10 ms. |
| `username` | `null` | `false` | The username to use for basic authentication. |
| `password` | `null` | `false` | The password to use for basic authentication. |