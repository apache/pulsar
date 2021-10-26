---
id: admin-api-permissions
title: Managing permissions
sidebar_label: "Permissions"
original_id: admin-api-permissions
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Permissions in Pulsar are managed at the [namespace](reference-terminology.md#namespace) level
(that is, within [tenants](reference-terminology.md#tenant) and [clusters](reference-terminology.md#cluster)).

## Grant permissions

You can grant permissions to specific roles for lists of operations such as `produce` and `consume`.

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
    "label": "Java",
    "value": "Java"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`grant-permission`](reference-pulsar-admin.md#grant-permission) subcommand and specify a namespace, actions using the `--actions` flag, and a role using the `--role` flag:

```shell

$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
  --actions produce,consume \
  --role admin10

```

Wildcard authorization can be performed when `authorizationAllowWildcardsMatching` is set to `true` in `broker.conf`.

e.g.

```shell

$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role 'my.role.*'

```

Then, roles `my.role.1`, `my.role.2`, `my.role.foo`, `my.role.bar`, etc. can produce and consume.  

```shell

$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role '*.role.my'

```

Then, roles `1.role.my`, `2.role.my`, `foo.role.my`, `bar.role.my`, etc. can produce and consume.

**Note**: A wildcard matching works at **the beginning or end of the role name only**.

e.g.

```shell

$ pulsar-admin namespaces grant-permission test-tenant/ns1 \
                        --actions produce,consume \
                        --role 'my.*.role'

```

In this case, only the role `my.*.role` has permissions.  
Roles `my.1.role`, `my.2.role`, `my.foo.role`, `my.bar.role`, etc. **cannot** produce and consume.

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/permissions/:role|operation/grantPermissionOnNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));

```

</TabItem>

</Tabs>

## Get permissions

You can see which permissions have been granted to which roles in a namespace.

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
    "label": "Java",
    "value": "Java"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`permissions`](reference-pulsar-admin#permissions) subcommand and specify a namespace:

```shell

$ pulsar-admin namespaces permissions test-tenant/ns1
{
  "admin10": [
    "produce",
    "consume"
  ]
}

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/permissions|operation/getPermissions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.namespaces().getPermissions(namespace);

```

</TabItem>

</Tabs>

## Revoke permissions

You can revoke permissions from specific roles, which means that those roles will no longer have access to the specified namespace.

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
    "label": "Java",
    "value": "Java"
  }
]}>
<TabItem value="pulsar-admin">

Use the [`revoke-permission`](reference-pulsar-admin.md#revoke-permission) subcommand and specify a namespace and a role using the `--role` flag:

```shell

$ pulsar-admin namespaces revoke-permission test-tenant/ns1 \
  --role admin10

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/permissions/:role|operation/revokePermissionsOnNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.namespaces().revokePermissionsOnNamespace(namespace, role);

```

</TabItem>

</Tabs>