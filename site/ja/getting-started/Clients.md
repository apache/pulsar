---
title: Pulsarクライアントライブラリ
---

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
