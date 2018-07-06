---
title: Pulsar Security Overview
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

Apache Pulsar is the central message bus for a business. It is frequently used to store mission-critical data, and therefore enabling security features are crucial.

By default, there is no encryption, authentication, or authorization configured. Any client can communicate to Apache Pulsar via plain text service urls.
It is critical that access via these plain text service urls is restricted to trusted clients only. Network segmentation and/or authorization ACLs can be used
to restrict access to trusted IPs in such cases. If neither is used, the cluster is wide open and can be accessed by anyone.

Pulsar supports a pluggable authentication mechanism that Pulsar clients can use to authenticate with {% popover brokers %} and {% popover proxies %}. Pulsar
can also be configured to support multiple authentication sources.

It is strongly recommended to secure the service components in your Apache Pulsar deployment.

## Role Tokens

In Pulsar, a *role* is a string, like `admin` or `app1`, that can represent a single client or multiple clients. Roles are used to control permission for clients
to produce or consume from certain topics, administer the configuration for {% popover tenants %}, and more.

Apache Pulsar uses a [Authentication Provider](#authentication-providers) to establish the identity of a client and then assign that client a *role token*. This
role token is then used for [Authorization and ACLs](../authorization) to determine what the client is authorized to do.

## Authentication Providers

Currently Pulsar supports two authentication providers:

* [TLS Authentication](../tls)
* [Athenz](../athenz)

## Contents

- [Encryption and Authentication using TLS](../tls)
- [Authentication using Athenz](../athenz)
- [Authorization and ACLs](../authorization)
- [End-to-End Encryption](../encryption)
