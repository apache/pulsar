---
title: Managing tenants
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

Tenants, like namespaces, can be managed using the [admin API](../../admin-api/overview). There are currently two configurable aspects of tenants:

* Admin roles
* Allowed clusters

## Tenant resources

### List

#### pulsar-admin

You can list all of the tenants associated with an {% popover instance %} using the [`list`](../../reference/CliTools#pulsar-admin-tenants-list) subcommand:

```shell
$ pulsar-admin tenants list
```

That will return a simple list, like this:

```
my-tenant-1
my-tenant-2
```

### Create

#### pulsar-admin

You can create a new tenant using the [`create`](../../reference/CliTools#pulsar-admin-tenants-create) subcommand:

```shell
$ pulsar-admin tenants create my-tenant
```

When creating a tenant, you can assign admin roles using the `-r`/`--admin-roles` flag. You can specify multiple roles as a comma-separated list. Here are some examples:

```shell
$ pulsar-admin tenants create my-tenant \
  --admin-roles role1,role2,role3

$ pulsar-admin tenants create my-tenant \
  -r role1
```

### Get configuration

#### pulsar-admin

You can see a tenant's configuration as a JSON object using the [`get`](../../reference/CliTools#pulsar-admin-tenants-get) subcommand and specifying the name of the tenant:

```shell
$ pulsar-admin tenants get my-tenant
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

#### pulsar-admin

You can delete a tenant using the [`delete`](../../reference/CliTools#pulsar-admin-tenants-delete) subcommand and specifying the tenant name:

```shell
$ pulsar-admin tenants delete my-tenant
```

### Updating

#### pulsar-admin

You can update a tenant's configuration using the [`update`](../../reference/CliTools#pulsar-admin-tenants-update) subcommand
