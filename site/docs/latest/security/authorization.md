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

In Pulsar, the [authentication provider](../../overview#authentication-providers) is charged with properly identifying clients and
associating them with [role tokens](../../overview#role-tokens). *Authorization* is the process that determines *what* clients are able to do.

Authorization in Pulsar is managed at the {% popover tenant %} level, which means that you can have multiple authorization schemes active
in a single Pulsar instance. You could, for example, create a `shopping` tenant that has one set of [roles](../../overview#role-tokens)
and applies to a shopping application used by your company, while an `inventory` tenant would be used only by an inventory application.

{% include message.html id="properties_multiple_clusters" %}

## Creating a new tenant

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

## Managing permissions

{% include explanations/permissions.md %}

## Superusers

In Pulsar you can assign certain roles to be *superusers* of the system. A superuser is allowed to perform all administrative tasks on all tenants and namespaces, as well as to publish and subscribe to all topics.

Superusers are configured in the broker configuration file in [`conf/broker.conf`](../../reference/Configuration#broker) configuration file, using the [`superUserRoles`](../../reference/Configuration#broker-superUserRoles) parameter:

```tenants
superUserRoles=my-super-user-1,my-super-user-2
```

{% include message.html id="broker_conf_doc" %}

Typically, superuser roles are used for administrators and clients but also for broker-to-broker authorization. When using [geo-replication](../GeoReplication), every broker
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
