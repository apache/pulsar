---
id: version-2.8.2-admin-api-permissions
title: Managing permissions
sidebar_label: Permissions
original_id: admin-api-permissions
---

> **Important**
>
> This page only shows **some frequently used operations**.
>
> - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more information, see [Pulsar admin doc](https://pulsar.apache.org/tools/pulsar-admin/).
> 
> - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
> 
> - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](https://pulsar.apache.org/api/admin/).

Pulsar allows you to grant namespace-level or topic-level permission to users.

- If you grant a namespace-level permission to a user, then the user can access all the topics under the namespace.

- If you grant a topic-level permission to a user, then the user can access only the topic.

The chapters below demonstrate how to grant namespace-level permissions to users. For how to grant topic-level permissions to users, see [manage topics](admin-api-topics.md/#grant-permission).

## Grant permissions

You can grant permissions to specific roles for lists of operations such as `produce` and `consume`.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

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

<!--REST API-->

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/permissions/:role|operation/grantPermissionOnNamespace?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Get permissions

You can see which permissions have been granted to which roles in a namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

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

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/permissions|operation/getPermissions?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().getPermissions(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Revoke permissions

You can revoke permissions from specific roles, which means that those roles will no longer have access to the specified namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`revoke-permission`](reference-pulsar-admin.md#revoke-permission) subcommand and specify a namespace and a role using the `--role` flag:

```shell
$ pulsar-admin namespaces revoke-permission test-tenant/ns1 \
  --role admin10
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/permissions/:role|operation/revokePermissionsOnNamespace?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role);
```
<!--END_DOCUSAURUS_CODE_TABS-->