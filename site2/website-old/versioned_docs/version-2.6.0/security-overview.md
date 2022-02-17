---
id: version-2.6.0-security-overview
title: Pulsar security overview
sidebar_label: Overview
original_id: security-overview
---

As the central message bus for a business, Apache Pulsar is frequently used for storing mission-critical data. Therefore, enabling security features in Pulsar is crucial.

By default, Pulsar configures no encryption, authentication, or authorization. Any client can communicate to Apache Pulsar via plain text service URLs. So we must ensure that Pulsar accessing via these plain text service URLs is restricted to trusted clients only. In such cases, you can use Network segmentation and/or authorization ACLs to restrict access to trusted IPs. If you use neither, the state of cluster is wide open and anyone can access the cluster.

Pulsar supports a pluggable authentication mechanism. And Pulsar clients use this mechanism to authenticate with brokers and proxies. You can also configure Pulsar to support multiple authentication sources.

You had better secure the service components in your Apache Pulsar deployment.

## Role tokens

In Pulsar, a *role* is a string, like `admin` or `app1`, which can represent a single client or multiple clients. You can use roles to control permission for clients to produce or consume from certain topics, administer the configuration for tenants, and so on.

Apache Pulsar uses a [Authentication Provider](#authentication-providers) to establish the identity of a client and then assign a *role token* to that client. This role token is then used for [Authorization and ACLs](security-authorization.md) to determine what the client is authorized to do.

## Authentication providers

Currently Pulsar supports the following authentication providers:

- [TLS Authentication](security-tls-authentication.md)
- [Athenz](security-athenz.md)
- [Kerberos](security-kerberos.md)
- [JSON Web Token Authentication](security-jwt.md)


