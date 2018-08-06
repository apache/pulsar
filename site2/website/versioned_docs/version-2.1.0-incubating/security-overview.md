---
id: version-2.1.0-incubating-security-overview
title: Pulsar Security Overview
sidebar_label: Overview
original_id: security-overview
---

Apache Pulsar is the central message bus for a business. It is frequently used to store mission-critical data, and therefore enabling security features are crucial.

By default, there is no encryption, authentication, or authorization configured. Any client can communicate to Apache Pulsar via plain text service urls.
It is critical that access via these plain text service urls is restricted to trusted clients only. Network segmentation and/or authorization ACLs can be used
to restrict access to trusted IPs in such cases. If neither is used, the cluster is wide open and can be accessed by anyone.

Pulsar supports a pluggable authentication mechanism that Pulsar clients can use to authenticate with brokers and proxies. Pulsar
can also be configured to support multiple authentication sources.

It is strongly recommended to secure the service components in your Apache Pulsar deployment.

## Role Tokens

In Pulsar, a *role* is a string, like `admin` or `app1`, that can represent a single client or multiple clients. Roles are used to control permission for clients
to produce or consume from certain topics, administer the configuration for tenants, and more.

Apache Pulsar uses a [Authentication Provider](#authentication-providers) to establish the identity of a client and then assign that client a *role token*. This
role token is then used for [Authorization and ACLs](security-authorization.md) to determine what the client is authorized to do.

## Authentication Providers

Currently Pulsar supports two authentication providers:

- [TLS Authentication](security-tls-authentication.md)
- [Athenz](security-athenz.md)

## Contents

- [Encryption](security-tls-transport.md) and [Authentication](security-tls-authentication.md) using TLS
- [Authentication using Athenz](security-athenz.md)
- [Authorization and ACLs](security-authorization.md)
- [End-to-End Encryption](security-encryption.md)

