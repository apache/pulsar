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

プロパティはネームスペースと同様に[admin API](../../admin/AdminInterface)を使用して管理できます。現在、プロパティの設定可能な要素は次の2つです:

* adminロール
* 許可されたクラスタ

### リストの取得

#### pulsar-admin

[`list`](../../reference/CliTools#pulsar-admin-properties-list)サブコマンドを使用して{% popover_ja インスタンス %}に関連付けられた全てのプロパティのリストを取得できます:

```shell
$ pulsar-admin properties list
```

これは次のようなシンプルなリストを返します:

```
my-property-1
my-property-2
```

### 作成

#### pulsar-admin

[`create`](../../reference/CliTools#pulsar-admin-properties-create)サブコマンドを使用して新しいプロパティを作成できます:

```shell
$ pulsar-admin properties create my-property
```

プロパティを作成する際、`-r`/`--admin-roles`オプションを使用してadminロールを割り当てる事ができます。カンマ区切りのリストとして複数のロールを指定する事も可能です。以下にいくつかの例を示します:

```shell
$ pulsar-admin properties create my-property \
  --admin-roles role1,role2,role3

$ pulsar-admin properties create my-property \
  -r role1
```

### 設定の取得

#### pulsar-admin

[`get`](../../reference/CliTools#pulsar-admin-properties-get)サブコマンドを使用し、プロパティの名前を指定する事でプロパティの設定をJSONオブジェクトとして参照できます:

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

### 削除

#### pulsar-admin

[`delete`](../../reference/CliTools#pulsar-admin-properties-delete)サブコマンドを使用し、プロパティの名前を指定する事でプロパティを削除できます:

```shell
$ pulsar-admin properties delete my-property
```

### 更新

#### pulsar-admin

[`update`](../../reference/CliTools#pulsar-admin-properties-update)サブコマンドを使用してプロパティの設定を更新できます。
