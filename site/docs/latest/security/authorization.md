---
title: Authentication and authorization in Pulsar
tags: [admin, authentication, authorization, athenz, tls, java, cpp]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

In Pulsar, the [authentication provider](../overview#authentication-providers) is charged with properly identifying clients and
associating them with [role tokens](../overview#role-tokens). If only authentication is enabled, an authenticated role token will have the ability to access all resources in the cluster. *Authorization* is the process that determines *what* clients are able to do.

The role tokens with the most privileges are the *superusers*. The *superusers* can create and destroy tenants, along with having full access to all tenant resources.

When a {% popover tenant %} is created by a superuser, that tenant is assigned an admin role. A client with the admin role token can then create, modify and destroy namespaces, and grant and revoke permissions to *other role tokens* on those namespaces.

## Broker and Proxy Setup

### Enabling Authorization and Assigning Superusers

Authorization is enabled and superusers are assigned in the broker ([`conf/broker.conf`](../../reference/Configuration#broker)) and proxy ([`conf/proxy.conf`](../../reference/Configuration#proxy)) configuration files.

```properties
authorizationEnabled=true
superUserRoles=my-super-user-1,my-super-user-2
```

{% include message.html id="broker_conf_doc" %}

Typically, superuser roles are used for administrators and clients but also for broker-to-broker authorization. When using [geo-replication](../GeoReplication), every broker needs to be able to publish to all the other clusters' topics.

### Proxy Roles

By default, the broker treats the connection between a proxy and the broker as a normal user connection. The user is authenticated as the role configured in ```proxy.conf``` (see ["Enabling TLS Authentication on Proxies"](../tls#-on-proxies)). However, this is rarely the behaviour that the user desires when connecting to the cluster through a proxy. The user expects to be able to interact with the cluster as the role for which they have authenticated with the proxy.

Pulsar uses *Proxy roles* to enable this. Proxy roles are specified in the broker configuration file, [`conf/broker.conf`](../../reference/Configuration#broker). If a client that is authenticated with a broker is one of its ```proxyRoles```, all requests from that client must also carry information about the role of the client that is authenticated with the proxy. If this information, which we call the *original principal*, is missing, the client will not be able to access anything.

Both the *proxy role* and the *original principle* must be authorized to access a resource for that resource to be accessible via the proxy. Administrators can take two approaches to this.

The more secure approach is to grant access to the proxy roles each time you grant access to a resource. For example, if you have a proxy role ```proxy1```, when a tenant is created by the superuser, ```proxy1``` should be specified as one of the admin roles. When a role is granted permissions to produce or consume from a namespace, if that client wants to produce or consume through a proxy, ```proxy1``` should also be granted the same permissions.

Another approach is to make the proxy role a superuser. This will allow the proxy to access all resources. The client will still need to authenticate with the proxy, and all requests made through the proxy will have their role downgraded to the *original principal* of the authenticated client. However, if the proxy is compromised, a bad actor could get full access to your cluster.

Roles can be specified as proxy roles in [`conf/broker.conf`](../../reference/Configuration#broker).

```properties
proxyRoles=my-proxy-role

# if you want to allow superusers to use the proxy (see above)
superUserRoles=my-super-user-1,my-super-user-2,my-proxy-role
```

## Administering Tenants

### Creating a new tenant

A Pulsar {% popover tenant %} is typically provisioned by Pulsar {% popover instance %} administrators or by some kind of self-service portal.

Tenants are managed using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool. Here's an example tenant creation command:

```shell
$ bin/pulsar-admin tenants create my-tenant \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east
```

This command will create a new tenant `my-tenant` that will be allowed to use the clusters `us-west` and `us-east`.

A client that successfully identified itself as having the role `my-admin-role` would then be allowed to perform all administrative tasks on this property.

The structure of topic names in Pulsar reflects the hierarchy between tenants, clusters, and namespaces:

{% include topic.html ten="tenant" n="namespace" t="topic" %}

### Managing permissions

{% include explanations/permissions.md %}

### Pulsar admin authentication

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin("pulsar+ssl://service:6651", config);
```

To use TLS:

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1:value1";
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin("pulsar+ssl://service:6651", config);
```
