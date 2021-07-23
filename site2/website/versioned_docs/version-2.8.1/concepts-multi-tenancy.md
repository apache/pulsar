---
id: version-2.8.1-concepts-multi-tenancy
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

### Namespace change events and topic-level policies

Pulsar is a multi-tenant event streaming system. Administrators can manage the tenants and namespaces by setting policies at different levels. However, the policies, such as retention policy and storage quota policy, are only available at a namespace level. In many use cases, users need to set a policy at the topic level. The namespace change events approach is proposed for supporting topic-level policies in an efficient way. In this approach, Pulsar is used as an event log to store namespace change events (such as topic policy changes). This approach has a few benefits:

- Avoid using ZooKeeper and introduce more loads to ZooKeeper.
- Use Pulsar as an event log for propagating the policy cache. It can scale efficiently.
- Use Pulsar SQL to query the namespace changes and audit the system.

Each namespace has a system topic `__change_events`. This system topic is used for storing change events for a given namespace. The following figure illustrates how to use namespace change events to implement a topic-level policy.

1. Pulsar Admin clients communicate with the Admin Restful API to update topic level policies.
2. Any broker that receives the Admin HTTP request publishes a topic policy change event to the corresponding `__change_events` topic of the namespace.
3. Each broker that owns a namespace bundle(s) subscribes to the `__change_events` topic to receive change events of the namespace. It then applies the change events to the policy cache.
4. Once the policy cache is updated, the broker sends the response back to the Pulsar Admin clients.
