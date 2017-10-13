---
title: Managing properties
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

Properties, like namespaces, can be managed using the [admin API](../../admin-api/overview). There are currently two configurable aspects of properties:

* Admin roles
* Allowed clusters


## Properties resources

### List

#### pulsar-admin

You can list all of the properties associated with an {% popover instance %} using the [`list`](../../reference/CliTools#pulsar-admin-properties-list) subcommand:

```shell
$ pulsar-admin properties list
```

That will return a simple list, like this:

```
my-property-1
my-property-2
```

### Create

#### pulsar-admin

You can create a new property using the [`create`](../../reference/CliTools#pulsar-admin-properties-create) subcommand:

```shell
$ pulsar-admin properties create my-property
```

When creating a property, you can assign admin roles using the `-r`/`--admin-roles` flag. You can specify multiple roles as a comma-separated list. Here are some examples:

```shell
$ pulsar-admin properties create my-property \
  --admin-roles role1,role2,role3

$ pulsar-admin properties create my-property \
  -r role1
```

### Get configuration

#### pulsar-admin

You can see a property's configuration as a JSON object using the [`get`](../../reference/CliTools#pulsar-admin-properties-get) subcommand and specifying the name of the property:

```shell
$ pulsar-admin properties get my-property
{
  "adminRoles": [
    "admin1",
    "admin2"
  ],
  "allowedClusters": [
    "cl1",
    "cl2"
  ]
}
```

### Delete

#### pulsar-adnin

You can delete a property using the [`delete`](../../reference/CliTools#pulsar-admin-properties-delete) subcommand and specifying the property name:

```shell
$ pulsar-admin properties delete my-property
```

### Updating

#### pulsar-admin

You can update a property's configuration using the [`update`](../../reference/CliTools#pulsar-admin-properties-update) subcommand
