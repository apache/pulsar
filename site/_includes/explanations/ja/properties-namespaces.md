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

Pulsarは{% popover_ja マルチテナント %}のシステムとして新規にデザインされました。Pulsarでは{% popover_ja テナント %}は[プロパティ](#プロパティ)によって識別されます。プロパティはPulsar{% popover_ja インスタンス %}内の最も上位の管理単位です。

### プロパティ

Pulsarインスタンス内の各プロパティごとに以下を割り当て可能です:

* [認可](../../admin/Authz#認可)スキーム
* プロパティが適用される{% popover_ja クラスタ %}のセット

### ネームスペース

{% popover_ja プロパティ %}と{% popover_ja ネームスペース %}は{% popover_ja マルチテナント %}をサポートするためのPulsarの2つのキーコンセプトです。

* **プロパティ**は{% popover_ja テナント %}を識別します。Pulsarは適切なキャパシティを割り当てられた特定のプロパティに対してプロビジョニングされます。
* **ネームスペース**はプロパティ内の管理単位の名称です。ネームスペースにセットされた設定ポリシーはそのネームスペース内に作成された全トピックに適用されます。プロパティはREST APIとCLIツールを使ったセルフ管理によって複数ネームスペースを作成することがあります。例えば、別々のアプリケーションを持つプロパティはアプリケーションごとに別のネームスペースを作成できます。

同じネームスペース内のトピック名は下記のようになります:

{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-1" %}
{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-2" %}
{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-3" %}
