---
id: security-authorization
title: Authentication and authorization in Pulsar
sidebar_label: Authorization and ACLs
---

In Pulsar, the [authentication provider](security-overview.md#authentication-providers) is charged with properly identifying clients and
associating them with [role tokens](security-overview.md#role-tokens). *Authorization* is the process that determines *what* clients are able to do.

Authorization in Pulsar is managed at the {% popover tenant %} level, which means that you can have multiple authorization schemes active
in a single Pulsar instance. You could, for example, create a `shopping` tenant that has one set of [roles](security-overview.md#role-tokens)
and applies to a shopping application used by your company, while an `inventory` tenant would be used only by an inventory application.

> When working with properties, you can specify which of your Pulsar clusters your property is allowed to use.
> This enables you to also have cluster-level authorization schemes.

## Creating a new tenant

A Pulsar {% popover tenant %} is typically provisioned by Pulsar {% popover instance %} administrators or by some kind of self-service portal.

Tenants are managed using the [`pulsar-admin`](reference-pulsar-admin.md) tool. Here's an example tenant creation command:

```shell
$ bin/pulsar-admin tenants create my-tenant \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east
```

This command will create a new tenant `my-tenant` that will be allowed to use the clusters `us-west` and `us-east`.

A client that successfully identified itself as having the role `my-admin-role` would then be allowed to perform all administrative tasks on this property.

The structure of topic names in Pulsar reflects the hierarchy between tenants, clusters, and namespaces:

```shell
persistent://tenant/namespace/topic
```

## Managing permissions

{% include explanations/permissions.md %}

## Superusers

In Pulsar you can assign certain roles to be *superusers* of the system. A superuser is allowed to perform all administrative tasks on all tenants and namespaces, as well as to publish and subscribe to all topics.

Superusers are configured in the broker configuration file in [`conf/broker.conf`](reference-configuration.md#broker) configuration file, using the [`superUserRoles`](reference-configuration.md#broker-superUserRoles) parameter:

```tenants
superUserRoles=my-super-user-1,my-super-user-2
```

> A full listing of parameters available in the `conf/broker.conf` file, as well as the default
> values for those parameters, can be found in [Broker Configuration](reference-configuration.md#broker).

Typically, superuser roles are used for administrators and clients but also for broker-to-broker authorization. When using [geo-replication](administration-geo.md), every broker
needs to be able to publish to other clusters' topics.

## Pulsar admin authentication

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```

To use TLS:

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```
