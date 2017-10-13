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

{% capture binary_release_url %}http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz{% endcapture %}
{% capture source_release_url %}http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-src.tar.gz{% endcapture %}


## システム要求

Pulsarは現在**MacOS**と**Linux**で利用可能です。Pulsarを利用するために、[Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)をインストールする必要があります。

## Pulsarのインストール

Pulsarを起動するために、以下のいずれかの方法でバイナリのtarballリリースをダウンロードしてください:

* 以下のボタンの1つをクリックすることによってダウンロードする:

  <a href="{{ source_release_url }}" class="download-btn btn btn-lg" role="button" aria-pressed="true">Pulsar {{ site.current_version }} source release</a>
  <a href="{{ binary_release_url }}" class="download-btn btn btn-lg" role="button" aria-pressed="true">Pulsar {{ site.current_version }} binary release</a>

* Pulsarの[ダウンロードページ](/download)からダウンロードする
* Pulsarの[リリースページ](https://github.com/apache/incubator-pulsar/releases/latest)からダウンロードする
* [wget](https://www.gnu.org/software/wget)を使ってダウンロードする:

  ```shell
  # ソースリリース
  $ wget http://archive.apache.org/dist/incubator/pulsar/pulsar-{{site.current_version}}/apache-pulsar-{{site.current_version}}-src.tar.gz

  # バイナリリリース
  $ wget http://archive.apache.org/dist/incubator/pulsar/pulsar-{{site.current_version}}/apache-pulsar-{{site.current_version}}-bin.tar.gz
  ```

tarballをダウンロードしたら、解凍して解凍結果のディレクトリに`cd`してください:

```bash
# ソースリリース
$ tar xvfz apache-pulsar-{{ site.current_version }}-src.tar.gz
$ cd apache-pulsar-{{ site.current_version }}

# バイナリリリース
$ tar xvfz apache-pulsar-{{ site.current_version }}-bin.tar.gz
$ cd apache-pulsar-{{ site.current_version }}
```

## パッケージに含まれるもの

ソースとバイナリパッケージ両方に以下のディレクトリが含まれています:

ディレクトリ | 含まれるもの
:---------|:--------
`bin` | [`pulsar`](../../reference/CliTools#pulsar)コマンドや[`pulsar-admin`](../../reference/CliTools#pulsar-admin)コマンドのような、Pulsarの[コマンドラインツール](../../reference/CliTools)
`conf` | [Brokerの設定](../../reference/Configuration#broker)や[ZooKeeperの設定](../../reference/Configuration#zookeeper)などPulsarの設定ファイル
`data` | {% popover_ja ZooKeeper %}や{% popover_ja BookKeeper %}が利用するデータストレージディレクトリ
`lib` | Pulsarによって使用される[JAR](https://ja.wikipedia.org/wiki/JAR_(%E3%83%95%E3%82%A1%E3%82%A4%E3%83%AB%E3%83%95%E3%82%A9%E3%83%BC%E3%83%9E%E3%83%83%E3%83%88))ファイル
`logs` | インストールによって作成されるログ

ソースパッケージには、[Pulsarリポジトリ]({{ site.pulsar_repo }})のバージョン{{ site.current_version}}固有のすべてのアセットが含まれています。
