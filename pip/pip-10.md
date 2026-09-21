# PIP-10: Remove cluster for namespace and topic names

* **Status**: Implemented
* **Author**: [Matteo Merli](https://github.com/merlimat)
* **Pull Request**: [#1150](https://github.com/apache/incubator-pulsar/pull/1150)
* **Mailing List discussion**:
* **Release**: 2.0.0


## Motivation

Currently in Pulsar there is a distinction between *local* and *global* topics,
where *global* topics are replicated and *local* topics are not.

A topic is *global* if it's created on a *global* namespace and *local* if it's
created on a namespace that it's tied to a particular Pulsar cluster.

For example:
 * Global namespace --> `my-tenant/global/my-namespace`
 * Local namespace --> `my-tenant/us-west/my-namespace`

Similarly, the topic names will follow as:

* Global topic --> `persistent://my-tenant/global/my-namespace/my-topic`
* Local topic --> `persistent://my-tenant/us-west/my-namespace/my-topic`

This distinction leads to a few confusing side effects:

 * Global it's kind of an overloaded term and everyone has a different view of it
 * If a user starts with *local* topic in a single cluster, later this cannot
   be converted into a *global* topic directly, because the topic name already
   include the particular cluster
 * Looking at the topic or namespace name, there is the wrong impression of
   a hierarchy between a tenant and a cluster, while in reality there is a
   many to many relationship between the two.

In reality, the difference between the two types is only coming from legacy
reason and there is no practical difference between a *global* with just
one single cluster in the replication list and a *local* namespace.

Given that *local* namespace is just a special case in the more general
*global* namespace, this proposal is to make all the namespaces to be
*global*.

Once all the namespaces are global, there will be no need to specify `global`
in the namespace or topic names. Thus the names could be simplified like in:

 * Namespace --> `my-tenant/my-namespace`
 * Topic --> `persistent://my-tenant/my-namespace/my-topic`

Existing namespaces and topics will continue work as before. All REST APIs and
tools will accept both naming schemes, though the documentation will just
refer to the new naming, to avoid confusion.


## Changes

 * `NamespaceName` and `DestinationName` are the only classes that are used to
    do the naming validation and will be updated to support both old and new
    scheme.
 * When creating a namespace we will add an option to immediately specify
   the replication clusters, to avoid multiple CLI commands or REST calls.
 * Admin API REST URL handlers will need to be adapted because they're based
   on expecting a certain number of `/` in the URL. New handlers will be added
   and the old ones will be marked as "hidden" for the auto-generated
   documentation in Swagger.
 * Examples and test will be converted to use the new convention. Most tests
   will not be converted at this point, to ensure both old and new scheme
   can coexist.
