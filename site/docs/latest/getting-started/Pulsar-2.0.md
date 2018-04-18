---
title: Pulsar 2.0
hide_pulsar2_notification: true
new: true
tags: ["2.0", "tenants", "clients"]
---

Pulsar 2.0 is a major new release for Pulsar that brings some bold changes to the platform, including [simplified topic names](#topic-names)

## New features in Pulsar 2.0

Feature | Description
:-------|:-----------
[Pulsar Functions](../../functions/overview) | A lightweight compute option for Pulsar

## Major changes

There are a few major changes that you should be aware of, as they may significantly impact your day-to-day usage.

### Properties versus tenants

Previously, Pulsar had a concept of {% popover properties %}. A property is essentially the exact same thing as a {% popover tenant %}, so the "property" terminology has been removed in version 2.0. The [`pulsar-admin properties`](../../CliTools#pulsar-admin) command-line interface, for example, has been replaced with the [`pulsar-admin tenants`](../../CliTools#pulsar-admin-tenants) interface. In some cases the properties terminology is still used but is now considered deprecated and will be removed entirely in a future release.

### Topic names

Prior to version 2.0, *all* Pulsar topics had the following form:

{% include topic.html type="{persistent|non-persistent}" ten="tenant" n="namespace" c="cluster" t="topic" %}

Two important changes have been made in Pulsar 2.0:

* There is no longer a [cluster component](#no-cluster-component)
* You can use a [flexible](#flexible-topic-naming) naming system to shorten many topic names

#### No cluster component

The {% popover cluster %} component has been removed from topic names. Thus, all topic names now have the following form:

{% include topic.html type="{persistent|non-persistent}" ten="tenant" n="namespace" t="topic" %}

#### Flexible topic naming

All topic names in Pulsar 2.0 ultimately have the form shown [above](#no-cluster-component) but you can now use shorthand names in many cases (for the sake of simplicity). The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace:

Topic aspect | Default
:------------|:-------
topic type | `persistent`
tenant | `public`
namespace | `default`


Because the default {% popover tenant %} is now `public` and the default {% popover namespace %} is `default`, you don't have to include those elements of the name if you want to use the defaults. Furthermore, the default topic type is [`persistent`](../ConceptsAndArchitecture#persistent-storage), so that part can be left out as well for persistent topics. The defaults are shown in the table below:


The table below shows some example topic name translations that use implicit defaults for topic type, tenant, and namespace:

Input topic name | Translated topic name
:----------------|:---------------------
`my-topic` | `persistent://public/default/my-topic`
`persistent://my-topic` | `persistent://public/default/my-topic`
`my-namespace/my-topic` | `persistent://public/my-namespace/my-topic`
`persistent://my-namespace/my-topic` | `persistent://public/my-namespace/my-topic`
`my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic`

{% include admonition.html type="warning" id="non-persistent-topic-names" content="
For [non-persistent topics](../ConceptsAndArchitecture#non-persistent-topics) you'll need to continue to specify the entire topic name, as the default-based rules for persistent topic names don't apply. Thus you cannot use a shorthand name like `non-persistent://my-topic` and would need to use `non-persistent://public/default/my-topic` instead." %}