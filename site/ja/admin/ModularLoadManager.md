---
title: モジュラロードマネージャ
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

{% javadoc ModularLoadManagerImpl broker org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl %}として実装されている*モジュラロードマネージャ*は、これまでに実装されていたロードマネージャ{% javadoc SimpleLoadManagerImpl broker org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl %}のより柔軟な代替手段です。これは複雑な負荷管理戦略を実装できるように抽象化を提供しつつ、負荷管理の方法をより簡単にしようと試みています。

## 使用方法

2通りの方法でモジュラロードマネージャを有効にできます:

1. `conf/broker.conf`の`loadManagerClassName`パラメータの値を`org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl`から`org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl`に変更します。
2. `pulsar-admin`ツールを使用します。以下はその例です:

   ```shell
   $ pulsar-admin update-dynamic-config \
     --config loadManagerClassName \
     --value org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl
   ```

   同じ方法で元々の値に戻す事も可能です。どちらの場合でも、ロードマネージャの指定に誤りがあればPulsarはデフォルトの`SimpleLoadManagerImpl`を使用します。

## 検証

どちらのロードマネージャが使用されているかを判断する方法はいくつかあります:

1. `pulsar-admin`を使用して`loadManagerClassName`要素を確認してください:

    ```shell
   $ bin/pulsar-admin brokers get-all-dynamic-config
   {
     "loadManagerClassName" : "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl"
   }
   ```

   `loadManagerClassName`要素が存在しなければ、デフォルトのロードマネージャが使用されます。

2. ZooKeeperの負荷レポートを参照してください。モジュラロードマネージャでは、`/loadbalance/brokers/...`の負荷レポートに多くの違いがあります。例えば`systemResourceUsage`の子要素 (`bandwidthIn`や`bandwidthOut`など) は全てトップレベルになります。以下はモジュラロードマネージャからの負荷レポートの例です:

    ```json
    {
      "bandwidthIn": {
        "limit": 10240000.0,
        "usage": 4.256510416666667
      },
      "bandwidthOut": {
        "limit": 10240000.0,
        "usage": 5.287239583333333
      },
      "bundles": [],
      "cpu": {
        "limit": 2400.0,
        "usage": 5.7353247655435915
      },
      "directMemory": {
        "limit": 16384.0,
        "usage": 1.0
      }
    }
    ```

    シンプルロードマネージャでは`/loadbalance/brokers/...`の負荷レポートは次のようになります:

    ```json
    {
      "systemResourceUsage": {
        "bandwidthIn": {
          "limit": 10240000.0,
          "usage": 0.0
        },
        "bandwidthOut": {
          "limit": 10240000.0,
          "usage": 0.0
        },
        "cpu": {
          "limit": 2400.0,
          "usage": 0.0
        },
        "directMemory": {
          "limit": 16384.0,
          "usage": 1.0
        },
        "memory": {
          "limit": 8192.0,
          "usage": 3903.0
        }
      }
    }
    ```

3. コマンドライン上で[Brokerの監視](../../reference/CliTools/#pulsar-perf-monitor-brokers)を行うと、使用されているロードマネージャの実装に応じて異なる形式で結果を出力します。

    以下はモジュラロードマネージャの例です:

    ```
    ===================================================================================================================
    ||SYSTEM         |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.00           |48.33          |0.01           |0.00           |0.00           |48.33          ||
    ||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||
    ||               |4              |4              |0              |2              |4              |0              ||
    ||LATEST         |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ||SHORT          |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ||LONG           |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ===================================================================================================================
    ```

    以下はシンプルロードマネージャの例です:

    ```
    ===================================================================================================================
    ||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||
    ||               |4              |4              |0              |2              |0              |0              ||
    ||RAW SYSTEM     |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.25           |47.94          |0.01           |0.00           |0.00           |47.94          ||
    ||ALLOC SYSTEM   |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.20           |1.89           |               |1.27           |3.21           |3.21           ||
    ||RAW MSG        |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.01           |0.01           |0.01           ||
    ||ALLOC MSG      |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |54.84          |134.48         |189.31         |126.54         |320.96         |447.50         ||
    ===================================================================================================================
    ```

モジュラロードマネージャは _中央集権的_ である事に注意する必要があります。つまり、バンドルを割り当てる全てのリクエストは---それを以前に経験したかあるいは初めてであるかに関わらず--- _リーダー_ Broker (時間と共に変更される可能性があります) によってのみ処理されます。現在のリーダーBrokerを調べるには、ZooKeeperの`/loadbalance/leader`ノードを確認してください。

## 実装

### データ

モジュラロードマネージャによって監視されるデータは{% javadoc LoadData broker org.apache.pulsar.broker.loadbalance.LoadData %}クラスに含まれています。利用可能なデータはバンドルデータとBrokerデータに細分化されます。

#### Broker

Brokerデータは{% javadoc BrokerData broker org.apache.pulsar.broker.BrokerData %}クラスに含まれます。それはさらに2つの部分に細分化され、1つは各BrokerがZooKeeperに書き込むローカルデータであり、もう1つはリーダーBrokerがZooKeeperに書き込むヒストリカルBrokerデータです。

##### ローカルBrokerデータ
ローカルBrokerデータは{% javadoc LocalBrokerData broker org.apache.pulsar.broker.LocalBrokerData %}に含まれ、次のリソースに関する情報を提供します:

* CPU使用率
* JVMのヒープメモリ使用率
* ダイレクトメモリ使用率
* 入出力それぞれの帯域幅使用率
* 全てのバンドルを合計した最新のメッセージの入出力レート
* トピック、バンドル、Producer、Consumerの総数
* このBrokerに割り当てられた全てのバンドルの名前
* このBrokerに対するバンドル割り当ての最新の変更

ローカルBrokerデータは"loadBalancerReportUpdateMaxIntervalMinutes"の設定に従って定期的に更新されます。いずれかのBrokerがそのローカルBrokerデータを更新した後、リーダーBrokerはZooKeeperのウォッチを通して直ちに更新を受け取ります。ローカルデータはZooKeeperのノード`/loadbalance/brokers/<broker host/port>`から読み込まれます。

##### ヒストリカルBrokerデータ

ヒストリカルBrokerデータは{% javadoc TimeAverageBrokerData broker org.apache.pulsar.broker.TimeAverageBrokerData %}クラスに含まれます。

定常状態における良好な決定および致命的な状態における反応的な決定を行う必要性を調整するために、ヒストリカルデータは2つの部分に分割されます。すなわち、反応的な決定のための短期データと定常的な決定のための長期データです。どちらの時間枠も次の情報を保持します:

* Broker全体のメッセージの入出力レート
* Broker全体のメッセージの入出力スループット

バンドルデータとは異なり、BrokerデータはグローバルなBrokerのメッセージレートとスループットのサンプルを保持しません。これらのサンプルは新しくバンドルが削除または追加されるため一定に保たれる事は期待できません。その代わりに、このデータはバンドルの短期データと長期データに集約されます。データがどのように収集・保持されているかを知るにはバンドルデータのセクションを参照してください。

ヒストリカルBrokerデータは、いずれかのBrokerがローカルデータをZooKeeperに書き込む度に、リーダーBrokerによってメモリ内で更新されます。そして、リーダーBrokerは`loadBalancerResourceQuotaUpdateIntervalMinutes`の設定に従って定期的にヒストリカルデータをZooKeeperに書き込みます。

##### バンドルデータ

バンドルデータは{% javadoc BundleData broker org.apache.pulsar.broker.BundleData %}に含まれます。ヒストリカルBrokerデータと同様に、バンドルデータも短期データと長期データに分けられます。それぞれの時間枠で保持される情報は次の通りです:

* このバンドルのメッセージの入出力レート
* このバンドルのメッセージの入出力スループット
* このバンドルの現在のサンプル数

前述の時間枠は、これらの値の平均を限られたサンプル数だけ保持する事によって実現されます。サンプルはローカルデータの中のメッセージレートとスループットから得られます。したがって、ローカルデータの更新間隔が2分、短期サンプルの数が10、長期サンプルの数が1000である場合、短期データは`10サンプル * 2分 / サンプル = 20分`だけ維持され、長期データは同様に2000分だけ維持されます。与えられた時間枠を満たすだけの十分なサンプルがない場合、存在するサンプルのみの平均がとられます。利用可能なサンプルがない場合、最初のサンプルによって上書きされるまでデフォルトの値が仮定されます。現在、デフォルトは次の値になっています:

* 入出力メッセージレート: 50 msg/sec
* 入出力メッセージスループット: 50 KB/sec

バンドルデータはいずれかのBrokerがローカルデータをZooKeeperに書き込む度に、リーダーBrokerのメモリ内で更新されます。そして、ヒストリカルBrokerデータと同様にリーダーBrokerは`loadBalancerResourceQuotaUpdateIntervalMinutes`の設定に従って定期的にバンドルデータをZooKeeperに書き込みます。

### トラフィック分散

モジュラロードマネージャは{% javadoc ModularLoadManagerStrategy broker org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy %}によって提供される抽象化を使用してバンドルの割り当てを決定します。戦略はサービスの設定、負荷データ全体、割り当てるバンドルのバンドルデータを考慮して決定を行います。現在サポートされている唯一の戦略は{% javadoc LeastLongTermMessageRate broker org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate %}だけですが、ユーザは望めば独自の戦略をすぐに盛り込む事ができます。

#### 最低長期メッセージレート戦略

その名前が示すように、最低長期メッセージレート戦略では各Brokerの長期時間ウィンドウの中のメッセージレートがほぼ均等になるようにバンドルをBrokerに分散させようとします。ただし、単純にメッセージレートに基づいて負荷を分散させるだけでは各Brokerのメッセージごとの非対称なリソース負担の問題には対応できません。したがって、割り当て処理ではCPU、メモリ、ダイレクトメモリ、入出力の帯域幅といったシステムリソースの使用率も考慮されます。これは`1 / (overload_threshold - max_usage)`に従った最終的なメッセージレートへの重み付けによって行われ、`overload_threshold`は`loadBalancerBrokerOverloadedThresholdPercentage`の設定に対応し、`max_usage`は割り当て候補となっているBrokerのシステムリソースの使用率の最大値です。この乗算によって、同じメッセージレートでより負荷の大きいマシンには割り当てられる負荷が小さくなるようにします。特に、ある1つのマシンが過負荷になった場合には、全てのマシンがほぼ過負荷になるようにします。Brokerの最大使用量が過負荷の閾値を超えた場合、そのBrokerへのバンドル割り当ては検討されません。全てのBrokerが過負荷の場合、バンドルはランダムに割り当てられます。
