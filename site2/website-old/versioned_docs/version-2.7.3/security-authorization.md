---
id: version-2.7.3-security-authorization
title: Authentication and authorization in Pulsar
sidebar_label: Authorization and ACLs
original_id: security-authorization
---


In Pulsar, the [authentication provider](security-overview.md#authentication-providers) is responsible for properly identifying clients and associating the clients with [role tokens](security-overview.md#role-tokens). If you only enable authentication, an authenticated role token has the ability to access all resources in the cluster. *Authorization* is the process that determines *what* clients are able to do.

The role tokens with the most privileges are the *superusers*. The *superusers* can create and destroy tenants, along with having full access to all tenant resources.

When a superuser creates a [tenant](reference-terminology.md#tenant), that tenant is assigned an admin role. A client with the admin role token can then create, modify and destroy namespaces, and grant and revoke permissions to *other role tokens* on those namespaces.

## Broker and Proxy Setup

### Enable authorization and assign superusers
You can enable the authorization and assign the superusers in the broker ([`conf/broker.conf`](reference-configuration.md#broker)) configuration files.

```properties
authorizationEnabled=true
superUserRoles=my-super-user-1,my-super-user-2
```

> A full list of parameters is available in the `conf/broker.conf` file.
> You can also find the default values for those parameters in [Broker Configuration](reference-configuration.md#broker). 

Typically, you use superuser roles for administrators, clients as well as broker-to-broker authorization. When you use [geo-replication](concepts-replication.md), every broker needs to be able to publish to all the other topics of clusters.

You can also enable the authorization for the proxy in the proxy configuration file (`conf/proxy.conf`). Once you enable the authorization on the proxy, the proxy does an additional authorization check before forwarding the request to a broker. 
If you enable authorization on the broker, the broker checks the authorization of the request when the broker receives the forwarded request.

### Proxy Roles

By default, the broker treats the connection between a proxy and the broker as a normal user connection. The broker authenticates the user as the role configured in `proxy.conf`(see ["Enable TLS Authentication on Proxies"](security-tls-authentication.md#enable-tls-authentication-on-proxies)). However, when the user connects to the cluster through a proxy, the user rarely requires the authentication. The user expects to be able to interact with the cluster as the role for which they have authenticated with the proxy.

Pulsar uses *Proxy roles* to enable the authentication. Proxy roles are specified in the broker configuration file, [`conf/broker.conf`](reference-configuration.md#broker). If a client that is authenticated with a broker is one of its ```proxyRoles```, all requests from that client must also carry information about the role of the client that is authenticated with the proxy. This information is called the *original principal*. If the *original principal* is absent, the client is not able to access anything.

You must authorize both the *proxy role* and the *original principal* to access a resource to ensure that the resource is accessible via the proxy. Administrators can take two approaches to authorize the *proxy role* and the *original principal*.

The more secure approach is to grant access to the proxy roles each time you grant access to a resource. For example, if you have a proxy role named `proxy1`, when the superuser creates a tenant, you should specify `proxy1` as one of the admin roles. When a role is granted permissions to produce or consume from a namespace, if that client wants to produce or consume through a proxy, you should also grant `proxy1` the same permissions.

Another approach is to make the proxy role a superuser. This allows the proxy to access all resources. The client still needs to authenticate with the proxy, and all requests made through the proxy have their role downgraded to the *original principal* of the authenticated client. However, if the proxy is compromised, a bad actor could get full access to your cluster.

You can specify the roles as proxy roles in [`conf/broker.conf`](reference-configuration.md#broker).

```properties
proxyRoles=my-proxy-role

# if you want to allow superusers to use the proxy (see above)
superUserRoles=my-super-user-1,my-super-user-2,my-proxy-role
```

## Administer tenants

Pulsar [instance](reference-terminology.md#instance) administrators or some kind of self-service portal typically provisions a Pulsar [tenant](reference-terminology.md#tenant). 

You can manage tenants using the [`pulsar-admin`](reference-pulsar-admin.md) tool. 

### Create a new tenant

The following is an example tenant creation command:

```shell
$ bin/pulsar-admin tenants create my-tenant \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east
```

This command creates a new tenant `my-tenant` that is allowed to use the clusters `us-west` and `us-east`.

A client that successfully identifies itself as having the role `my-admin-role` is allowed to perform all administrative tasks on this tenant.

The structure of topic names in Pulsar reflects the hierarchy between tenants, clusters, and namespaces:

```shell
persistent://tenant/namespace/topic
```

### Manage permissions

You can use [Pulsar Admin Tools](admin-api-permissions.md) for managing permission in Pulsar.

### Pulsar admin authentication

```java
PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl("http://broker:8080")
                    .authentication("com.org.MyAuthPluginClass", "param1:value1")
                    .build();
```

To use TLS:

```java
PulsarAdmin admin = PulsarAdmin.builder()
                    .serviceHttpUrl("https://broker:8080")
                    .authentication("com.org.MyAuthPluginClass", "param1:value1")
                    .tlsTrustCertsFilePath("/path/to/trust/cert")
                    .build();
```
