---
id: admin-api-overview
title: Pulsar admin interfaces
sidebar_label: "Overview"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


The Pulsar admin interface enables you to manage all important entities in a Pulsar instance, such as tenants, topics, and namespaces.

You can interact with the admin interface via:

- The `pulsar-admin` CLI tool, which is available in the `bin` folder of your Pulsar installation:

  ```shell
  bin/pulsar-admin
  ```

  :::tip
   
  For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more information, see [Pulsar admin doc](/tools/pulsar-admin/).

  [Pulsar Shell](administration-pulsar-shell.md) extends `pulsar-admin` with an improved user experience for more flexibility and easier navigation between multiple clusters.
  
  :::

- HTTP calls, which are made against the admin {@inject: rest:REST:/} API provided by Pulsar brokers. For some RESTful APIs, they might be redirected to the owner brokers for serving with [`307 Temporary Redirect`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307), hence the HTTP callers should handle `307 Temporary Redirect`. If you use `curl` commands, you should specify `-L` to handle redirections.
  
  :::tip
  
  For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

  :::

- A Java client interface.
  
  :::tip
   
  For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

  :::
  
> **The REST API is the admin interface**. Both the `pulsar-admin` CLI tool and the Java client use the REST API. If you implement your own admin interface client, you should use the REST API. 