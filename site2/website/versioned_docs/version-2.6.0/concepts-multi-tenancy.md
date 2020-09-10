---
id: version-2.6.0-concepts-multi-tenancy
title: Multi Tenancy
sidebar_label: Multi Tenancy
original_id: concepts-multi-tenancy
---

Pulsar was created from the ground up as a multi-tenant system. To support multi-tenancy, Pulsar has a concept of tenants. Tenants can be spread across clusters and can each have their own [authentication and authorization](security-overview.md) scheme applied to them. They are also the administrative unit at which storage quotas, [message TTL](cookbooks-retention-expiry.md#time-to-live-ttl), and isolation policies can be managed.

The multi-tenant nature of Pulsar is reflected mostly visibly in topic URLs, which have this structure:

```http
persistent://tenant/namespace/topic
```

As you can see, the tenant is the most basic unit of categorization for topics (more fundamental than the namespace and topic name).

## Tenants

To each tenant in a Pulsar instance you can assign:

* An [authorization](security-authorization.md) scheme
* The set of [clusters](reference-terminology.md#cluster) to which the tenant's configuration applies

## Namespaces

Tenants and namespaces are two key concepts of Pulsar to support multi-tenancy.

* Pulsar is provisioned for specified tenants with appropriate capacity allocated to the tenant.
* A namespace is the administrative unit nomenclature within a tenant. The configuration policies set on a namespace apply to all the topics created in that namespace. A tenant may create multiple namespaces via self-administration using the REST API and the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool. For instance, a tenant with different applications can create a separate namespace for each application.

Names for topics in the same namespace will look like this:

```http
persistent://tenant/app1/topic-1

persistent://tenant/app1/topic-2

persistent://tenant/app1/topic-3
```
