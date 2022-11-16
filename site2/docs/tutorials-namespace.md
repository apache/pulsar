---
Id: tutorials-namespace
title: How to create a namespace
sidebar_label: “Tutorials”
---


Pulsar namespaces are logical groupings of topics.

Namespaces can be managed via:

- The namespaces command of the pulsar-admin tool
- The /admin/v2/namespaces endpoint of the admin {@inject: rest:REST:/} API
- The namespaces method of the PulsarAdmin object in the Java API

In this tutorial, we create a namespace called pulsar in the tenant apache. Then we list namespaces of tenant apache to see if the namespace is created successfully.

Create the namespace.

```bash
bin/pulsar-admin namespaces create apache/pulsar
```

Verify the namespace.

```bash
bin/pulsar-admin namespaces list apache
```

You should see similar output to show the namespace apache/pulsar has been successfully created.

#### Related Topics

- [Set up a tenant](tutorials-tenant.md)
- [Create a topic](tutorials-topic.md)
- [Produce and consume messages](tutorials-produce-consume.md)
- [Manage clusters](admin-api-clusters.md)







