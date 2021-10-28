---
id: concepts-authentication
title: Authentication and Authorization
sidebar_label: Authentication and Authorization
original_id: concepts-authentication
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Pulsar supports a pluggable [authentication](security-overview.md) mechanism which can be configured at the proxy and/or the broker. Pulsar also supports a pluggable [authorization](security-authorization) mechanism. These mechanisms work together to identify the client and its access rights on topics, namespaces and tenants.

