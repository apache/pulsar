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

{% popover_ja BookKeeper %}はPulsarの[永続メッセージストレージ](../../getting-started/ConceptsAndArchitecture#永続ストレージ)です。 

Pulsarの各{% popover_ja Broker %}は専用の{% popover_ja Bookie %}クラスタが必要です。BookKeeperのクラスタはPulsarクラスタとLocal ZooKeeperのクォーラムを共有します。

### Bookieの設定

Bookieは[`conf/bookkeeper.conf`](../../reference/Configuration#bookkeeper)ファイルを使って設定できます。各Bookieの設定で最も重要な点は[`zkServers`](../../reference/Configuration#bookkeeper-zkServers)パラメータにPulsarクラスタのLocal ZooKeeperをカンマで繋げた文字列が設定されていることを確認することです。 

### Bookieの起動

2つの方法でBookieを起動できます。すなわち、フォアグラウンドで起動する方法とバックグラウンドデーモンとして起動する方法です。

バックグラウンドデーモンとしてBookieを起動するには[`pulsar-daemon`](../../reference/CliTools#pulsar-daemon)ツールを使います

```shell
$ bin/pulsar-daemon start bookie
```

[BookKeeper shell](../../reference/CliTools#bookkeeper-shell)の`bookiesanity`コマンドでBookieが起動しているか確認できます:

```shell
$ bin/bookkeeper shell bookiesanity
```

これはローカルのBookie上に新しいLedgerを作成し、いくつかのエントリを追加し、それらを読み込み、最終的には作成したLedgerを削除します。

### ハードウェアの考慮事項

{% popover_ja Bookie %}はディスク上にメッセージデータを保存する役割があります。Bookieでは最適なパフォーマンスを提供するために、適切なハードウェア構成が不可欠です。Bookieのハードウェア能力には重要な2つの要素があります:

* 読み書きに対するディスクI/O性能
* ストレージの容量

Bookieに書き込まれるメッセージのエントリはPulsarの{% popover_ja Broker %}にAck (確認応答) を返す前にディスクに同期されます。書き込みのレイテンシを低くするために、BookKeeperは複数のディスクを使用するようにデザインされています:

* **Journal**は耐久性を確保します。シーケンシャルな書き込みに対して、Bookie上で高速な[fsync](https://linux.die.net/man/2/fsync)の操作は重要です。一般的に容量は小さいが高速な[ソリッドステートドライブ](https://ja.wikipedia.org/wiki/%E3%82%BD%E3%83%AA%E3%83%83%E3%83%89%E3%82%B9%E3%83%86%E3%83%BC%E3%83%88%E3%83%89%E3%83%A9%E3%82%A4%E3%83%96) (SSD) か、[RAID](https://ja.wikipedia.org/wiki/RAID)コントローラとバッテリバックアップ式のライトキャッシュを備えた[ハードディスクドライブ](https://ja.wikipedia.org/wiki/%E3%83%8F%E3%83%BC%E3%83%89%E3%83%87%E3%82%A3%E3%82%B9%E3%82%AF%E3%83%89%E3%83%A9%E3%82%A4%E3%83%96) (HDD) が適しています。どちらのソリューションもfsyncのレイテンシは0.4 ms以下に抑えることができます。
* **Ledger用のストレージデバイス**には全ての{% popover_ja Consumer %}が{% popover_ja Ack %}を返すまでデータが保持されます。書き込みはバックグラウンドで行われるため、書き込みI/Oは大きな問題ではありません。読み込みはほとんどの場合にシーケンシャルに行われ、バックログはConsumerに排出する場合にのみ排出されます。一般的な構成では大量のデータを保存できるようにRAIDコントローラを備えた複数のHDDを使用します。
