# PIP-11: Short topic names

* **Status**: Implemented
* **Author**: [Matteo Merli](https://github.com/merlimat), [Sijie Guo](https://github.com/sijie)
* **Pull Request**: [#1536](https://github.com/apache/incubator-pulsar/pull/1535)
* **Mailing List discussion**:


## Motivation

Pulsar was designed as a multi-tenant system from the very beginning. From this
it originate the notions of tenants and namespaces to allow users to administrate
and operate their own topics.

In Pulsar, all these concepts are explicitly set in the namespace and topic names.

The topic name is composed of all the names:

```
persistent://my-tenant/my-cluster/my-namespace/my-topic
```

In [PIP-10](https://github.com/apache/incubator-pulsar/wiki/PIP-10:-Remove-cluster-for-namespace-and-topic-names), we have already introduced the proposal to hide the cluster name from
the topic name, resulting in a topic name such as:

```
persistent://my-tenant/my-namespace/my-topic
```

This proposal is dependent on PIP-10 because it needs the cluster to not appear
in the topic name.

In many cases, when deploying Pulsar as a single tenant, users might not need
all the "tenant" and "namespace" information, but currently it would have to
do the following steps after deploying a Pulsar cluster, even when authorization
is not required:

 * Create a "property" (or tenant)
 * Create a namespace

Additionally, it forces the multi-tenant model even when it's not needed or
when someone wants to just "play" with the system.

For such reasons, this proposal is to introduce a default namespace that is
always pre-created and a short notation for topic names.

For example:

 * `my-topic` --> `persistent://public/default/my-topic`

If omitted, the `persistent` or `non-persistent` schema, will be defaulted to
`persistent`, even when using longer form of topic name:

 * `tenant/namespace/my-topic` --> `persistent://tenant/namespace/my-topic`

The goal is to be able to use the short form in client API and tools:

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");
Producer producer = client.createProducer("my-topic");
producer.send(message);
```

At the same time, all CLI tools will recognize the short notation as well,
for example:

```shell
$ bin/pulsar-admin persistent stats my-topic
```

It will be equivalent to:

```shell
$ bin/pulsar-admin persistent stats persistent://public/default/my-topic
```


## Changes

The changes will be mostly on the surface:

 * Introduce the concept of `public` tenant (property). This tenant will be
   already pre-created when initializing a Pulsar cluster. When authorization
   is enabled, it would be responsibility of the administrator to grant
   permissions to use it.
 * Introduce the concept of `public/default` namespace. This will be pre-created
   at the cluster initialization. The default namespace will be a regular
   namespace that the administrator can configure in the same way as all other
   namespaces
 * In client library and tools, translate all the short topic names by adding
   the default namespace:
    `my-topic` --> `persistent://public/default/my-topic`
