---
id: admin-api-tenants
title: Managing Tenants
sidebar_label: "Tenants"
original_id: admin-api-tenants
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Tenants, like namespaces, can be managed using the [admin API](admin-api-overview). There are currently two configurable aspects of tenants:

* Admin roles
* Allowed clusters

## Tenant resources

### List

You can list all of the tenants associated with an [instance](reference-terminology.md#instance).

<Tabs 
  defaultValue="pulsar-admin"
  values={[
  {
    "label": "pulsar-admin",
    "value": "pulsar-admin"
  },
  {
    "label": "REST API",
    "value": "REST API"
  },
  {
    "label": "JAVA",
    "value": "JAVA"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`list`](reference-pulsar-admin.md#tenants-list) subcommand.

```shell

$ pulsar-admin tenants list
my-tenant-1
my-tenant-2

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/tenants|operation/getTenants?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

```java

admin.tenants().getTenants();

```

</TabItem>

</Tabs>

### Create

You can create a new tenant.

<Tabs 
  defaultValue="pulsar-admin"
  values={[
  {
    "label": "pulsar-admin",
    "value": "pulsar-admin"
  },
  {
    "label": "REST API",
    "value": "REST API"
  },
  {
    "label": "JAVA",
    "value": "JAVA"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`create`](reference-pulsar-admin.md#tenants-create) subcommand:

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

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/tenants/:tenant|operation/createTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

```java

admin.tenants().createTenant(tenantName, tenantInfo);

```

</TabItem>

</Tabs>

### Get configuration

You can fetch the [configuration](reference-configuration) for an existing tenant at any time.

<Tabs 
  defaultValue="pulsar-admin"
  values={[
  {
    "label": "pulsar-admin",
    "value": "pulsar-admin"
  },
  {
    "label": "REST API",
    "value": "REST API"
  },
  {
    "label": "JAVA",
    "value": "JAVA"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`get`](reference-pulsar-admin.md#tenants-get) subcommand and specify the name of the tenant. Here's an example:

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

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/tenants/:cluster|operation/getTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

```java

admin.tenants().getTenantInfo(tenantName);

```

</TabItem>

</Tabs>

### Delete

Tenants can be deleted from a Pulsar [instance](reference-terminology.md#instance).

<Tabs 
  defaultValue="pulsar-admin"
  values={[
  {
    "label": "pulsar-admin",
    "value": "pulsar-admin"
  },
  {
    "label": "REST API",
    "value": "REST API"
  },
  {
    "label": "JAVA",
    "value": "JAVA"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`delete`](reference-pulsar-admin.md#tenants-delete) subcommand and specify the name of the tenant.

```shell

$ pulsar-admin tenants delete my-tenant

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/tenants/:cluster|operation/deleteTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

```java

admin.Tenants().deleteTenant(tenantName);

```

</TabItem>

</Tabs>

### Update

You can update a tenant's configuration.

<Tabs 
  defaultValue="pulsar-admin"
  values={[
  {
    "label": "pulsar-admin",
    "value": "pulsar-admin"
  },
  {
    "label": "REST API",
    "value": "REST API"
  },
  {
    "label": "JAVA",
    "value": "JAVA"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`update`](reference-pulsar-admin.md#tenants-update) subcommand.

```shell

$ pulsar-admin tenants update my-tenant

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/tenants/:cluster|operation/updateTenant?version=@pulsar:version_number@}

</TabItem>
<TabItem value="JAVA">

```java

admin.tenants().updateTenant(tenantName, tenantInfo);

```

</TabItem>

</Tabs>
