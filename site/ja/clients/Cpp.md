---
title: Pulsar C++クライアント
tags_ja: [client, cpp]
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

<!-- source: https://github.com/apache/incubator-Âpulsar/tree/master/pulsar-client-cpp -->

{% include admonition.html type='info' content="オープンソースコミュニティからの貢献を歓迎します。変更はgcc-4.4.7およびBoost 1.41と下位互換性を持たせてください。" %}

## サポートするプラットフォーム

**MacOS**と**Linux**上で動作確認されています。

## システム要件

C++クライアントを利用するためには下記ソフトウェアをインストールする必要があります:

* [CMake](https://cmake.org/)
* [Boost](http://www.boost.org/)
* [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.6
* [Log4CXX](https://logging.apache.org/log4cxx)
* [libcurl](https://curl.haxx.se/libcurl/)
* [Google Test](https://github.com/google/googletest)
* [JsonCpp](https://github.com/open-source-parsers/jsoncpp)

## コンパイル

[MacOS](#macos)と[Linux](#linux)には別々のコンパイル方法があります。両システムとも、Pulsarリポジトリをクローンすることから始めます:

```shell
$ git clone {{ site.pulsar_repo }}
```

### Linux

初めに必要な依存パッケージを全てインストールします:

```shell
$ apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
  libprotobuf-dev libboost-all-dev libgtest-dev libjsoncpp-dev
```

その時に[Google Test](https://github.com/google/googletest)をコンパイル、インストールします:

```shell
$ git clone https://github.com/google/googletest.git && cd googletest
$ sudo cmake .
$ sudo make
$ sudo cp *.a /usr/lib
```

最後にPulsarリポジトリ内のC++クライアントライブラリをコンパイルします:

```shell
$ cd pulsar-client-cpp
$ cmake .
$ make
```

結果ファイルとして、`libpulsar.so`と`libpulsar.a`がリポジトリ内のlibディレクトリ内に配置されます。同時に`perfProducer`と`perfConsumer`の2ツールがperfディレクトリ内に配置されます。

### MacOS

初めに必要な依存パッケージを全てインストールします:

```shell
# OpenSSLのインストール
$ brew install openssl
$ export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include/
$ export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/

# Protocol Buffersのインストール
$ brew tap homebrew/versions
$ brew install protobuf260
$ brew install boost
$ brew install log4cxx

# Google Testのインストール
$ git clone https://github.com/google/googletest.git
$ cd googletest
$ cmake .
$ make install
```

次にPulsarリポジトリ内のC++クライアントライブラリをコンパイルします:

```shell
$ cd pulsar-client-cpp
$ cmake .
$ make
```

## 接続URL

{% include explanations/ja/client-url.md %}

## Consumer

```c++
Client client("pulsar://localhost:6650");

Consumer consumer;
Result result = client.subscribe("persistent://sample/standalone/ns1/my-topic", "my-subscribtion-name", consumer);
if (result != ResultOk) {
    LOG_ERROR("Failed to subscribe: " << result);
    return -1;
}

Message msg;

while (true) {
    consumer.receive(msg);
    LOG_INFO("Received: " << msg << "  with payload '" << msg.getDataAsString() << "'");

    consumer.acknowledge(msg);
}

client.close();
```


## Producer

```cpp
Client client("pulsar://localhost:6650");

Producer producer;
Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}

// トピックに10個のメッセージを発行
for(int i=0;i<10;i++){
    Message msg = MessageBuilder().setContent("my-message").build();
    Result res = producer.send(msg);
    LOG_INFO("Message sent: " << res);
}
client.close();
```

## 認証

```cpp
ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);
std::string certfile = "/path/to/cacert.pem";

ParamMap params;
params["tlsCertFile"] = "/path/to/client-cert.pem";
params["tlsKeyFile"]  = "/path/to/client-key.pem";
config.setTlsTrustCertsFilePath(certfile);
config.setTlsAllowInsecureConnection(false);
AuthenticationPtr auth = pulsar::AuthFactory::create("/path/to/libauthtls.so", params);
config.setAuth(auth);

Client client("pulsar+ssl://my-broker.com:6651",config);
```
