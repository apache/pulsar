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

Pulsarにおけるパーミッションは{% popover_ja ネームスペース %}レベル (つまり{% popover_ja プロパティ %}および{% popover_ja クラスタ %}内) で管理されます。

### パーミッションの付与

`produce`や`consume`のような操作のリストに対して、特定のロールにパーミッションを与える事ができます。

#### pulsar-admin

[`grant-permission`](../../reference/CliTools#pulsar-admin-namespaces-grant-permission)サブコマンドを使用し、ネームスペースと、`--actions`オプションでアクションを、`--role`オプションでロールを指定してください:

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
  --actions produce,consume \
  --role admin10
```

`broker.conf`で`authorizationAllowWildcardsMatching`が`true`に設定されている場合、ワイルドカードを用いた認可が行われます。

例:

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role 'my.role.*'
```

この時、`my.role.1`, `my.role.2`, `my.role.foo`, `my.role.bar`などのロールがproduce/consume可能となります。

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role '*.role.my'
```

この時、`1.role.my`, `2.role.my`, `foo.role.my`, `bar.role.my`などのロールがproduce/consume可能となります。

**注意**: ワイルドカードを用いたマッチングは**ロール名の先頭または末尾でのみ**機能します。

例:

```shell
$ pulsar-admin namespaces grant-permission test-property/cl1/ns1 \
                        --actions produce,consume \
                        --role 'my.*.role'
```

この場合、ロール`my.*.role`のみがパーミッションを持っています。`my.1.role`, `my.2.role`, `my.foo.role`, `my.bar.role`のようなロールはproduce/consumeが**できません**。

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions));
```

### パーミッションの取得

ネームスペースにおいて、どのロールにどのパーミッションが与えられているかを確認できます。

#### pulsar-admin

[`permissions`](../../reference/CliTools#pulsar-admin-namespaces-permissions)サブコマンドを使用しネームスペースを指定してください:

```shell
$ pulsar-admin namespaces permissions test-property/cl1/ns1
{
  "admin10": [
    "produce",
    "consume"
  ]
}   
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/permissions %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions)

#### Java

```java
admin.namespaces().getPermissions(namespace);
```

### パーミッションの剥奪

特定のロールからパーミッションを剥奪できます。つまり、そのロールは指定されたネームスペースにアクセスできなくなります。

#### pulsar-admin

[`revoke-permission`](../../reference/CliTools#pulsar-admin-revoke-permission)サブコマンドを使用し、ネームスペースと、`--role`オプションでロールを指定してください:

```shell
$ pulsar-admin namespaces revoke-permission test-property/cl1/ns1 \
  --role admin10
```

#### REST API

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role);
```
