---
title: Pulsarクライアントライブラリ
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

Pulsarのクライアントライブラリは現在3つの言語に対応しています:

* [Java](#javaクライアント)
* [Python](#pythonクライアント)
* [C++](#cクライアント)

## Javaクライアント

PulsarのJavaクライアントを使用してメッセージをproduce, consumeするためのチュートリアルとして、[Pulsar Javaクライアント](../../clients/Java)をご確認ください。

2つの独立した Javadoc API ドキュメントセットが利用可能です:

ライブラリ | 利用目的
:-------|:-------
[`org.apache.pulsar.client.api`](/api/client) | Pulsarの{% popover_ja トピック %}上でメッセージをproduce, consumeするために利用する[Pulsar Javaクライアント](../../clients/Java)
[`org.apache.pulsar.client.admin`](/api/admin) | [Pulsar admin API](../../admin/AdminInterface)のJavaクライアント

<!-- * [`com.yahoo.pulsar.broker`](/api/broker) -->

## Pythonクライアント

PulsarのPythonクライアントを使うためのチュートリアルとして、[Pulsar Pythonクライアント](../../clients/Python)をご確認ください。

[pdoc](https://github.com/BurntSushi/pdoc)によって生成された[PythonクライアントのAPIドキュメント](/api/python)もあります。

## C++クライアント

PulsarのC++クライアントを使うためのチュートリアルとして、[Pulsar C++クライアント](../../clients/Cpp)をご確認ください。

[Doxygen](http://www.stack.nl/~dimitri/doxygen/)によって生成された[C++クライアントのAPIドキュメント]({{ site.baseurl }}api/cpp)もあります。
