---
id: io-solr-sink
title: Solr sink connector
sidebar_label: "Solr sink connector"
original_id: io-solr-sink
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


The Solr sink connector pulls messages from Pulsar topics 
and persists the messages to Solr collections.



## Configuration

The configuration of the Solr sink connector has the following properties.



### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `solrUrl` | String|true|" " (empty string) | <li>Comma-separated zookeeper hosts with chroot used in the SolrCloud mode. <br />**Example**<br />`localhost:2181,localhost:2182/chroot` <br /><br /></li><li>URL to connect to Solr used in standalone mode. <br />**Example**<br />`localhost:8983/solr` </li>|
| `solrMode` | String|true|SolrCloud| The client mode when interacting with the Solr cluster. <br /><br />Below are the available options:<br /><li>Standalone<br /></li><li> SolrCloud</li>|
| `solrCollection` |String|true| " " (empty string) | Solr collection name to which records need to be written. |
| `solrCommitWithinMs` |int| false|10 | The time within million seconds for Solr updating commits.|
| `username` |String|false|  " " (empty string) | The username for basic authentication.<br /><br />**Note: `usename` is case-sensitive.** |
| `password` | String|false|  " " (empty string) | The password for basic authentication. <br /><br />**Note: `password` is case-sensitive.** |



### Example

Before using the Solr sink connector, you need to create a configuration file through one of the following methods.

* JSON

  ```json
  
  {
      "solrUrl": "localhost:2181,localhost:2182/chroot",
      "solrMode": "SolrCloud",
      "solrCollection": "techproducts",
      "solrCommitWithinMs": 100,
      "username": "fakeuser",
      "password": "fake@123"
  }
  
  ```

* YAML

  ```yaml
  
  {
      solrUrl: "localhost:2181,localhost:2182/chroot"
      solrMode: "SolrCloud"
      solrCollection: "techproducts"
      solrCommitWithinMs: 100
      username: "fakeuser"
      password: "fake@123"
  }
  
  ```

