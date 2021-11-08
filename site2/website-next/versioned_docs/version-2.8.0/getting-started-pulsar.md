---
id: pulsar-2.0
title: Pulsar 2.0
sidebar_label: "Pulsar 2.0"
original_id: pulsar-2.0
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Pulsar 2.0 is a major new release for Pulsar that brings some bold changes to the platform, including [simplified topic names](#topic-names), the addition of the [Pulsar Functions](functions-overview) feature, some terminology changes, and more.

## New features in Pulsar 2.0

Feature | Description
:-------|:-----------
[Pulsar Functions](functions-overview) | A lightweight compute option for Pulsar

## Major changes

There are a few major changes that you should be aware of, as they may significantly impact your day-to-day usage.

### Properties versus tenants

Previously, Pulsar had a concept of properties. A property is essentially the exact same thing as a tenant, so the "property" terminology has been removed in version 2.0. The [`pulsar-admin properties`](reference-pulsar-admin.md#pulsar-admin) command-line interface, for example, has been replaced with the [`pulsar-admin tenants`](reference-pulsar-admin.md#pulsar-admin-tenants) interface. In some cases the properties terminology is still used but is now considered deprecated and will be removed entirely in a future release.

### Topic names

Prior to version 2.0, *all* Pulsar topics had the following form:

```http

{persistent|non-persistent}://property/cluster/namespace/topic

```

Two important changes have been made in Pulsar 2.0:

* There is no longer a [cluster component](#no-cluster)
* Properties have been [renamed to tenants](#tenants)
* You can use a [flexible](#flexible-topic-naming) naming system to shorten many topic names
* `/` is not allowed in topic name

#### No cluster component

The cluster component has been removed from topic names. Thus, all topic names now have the following form:

```http

{persistent|non-persistent}://tenant/namespace/topic

```

> Existing topics that use the legacy name format will continue to work without any change, and there are no plans to change that.


#### Flexible topic naming

All topic names in Pulsar 2.0 internally have the form shown [above](#no-cluster-component) but you can now use shorthand names in many cases (for the sake of simplicity). The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace:

Topic aspect | Default
:------------|:-------
topic type | `persistent`
tenant | `public`
namespace | `default`

The table below shows some example topic name translations that use implicit defaults:

Input topic name | Translated topic name
:----------------|:---------------------
`my-topic` | `persistent://public/default/my-topic`
`my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic`

> For [non-persistent topics](concepts-messaging.md#non-persistent-topics) you'll need to continue to specify the entire topic name, as the default-based rules for persistent topic names don't apply. Thus you cannot use a shorthand name like `non-persistent://my-topic` and would need to use `non-persistent://public/default/my-topic` instead

