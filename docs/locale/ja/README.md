***訳注: [v1.16のドキュメント](https://github.com/yahoo/pulsar/tree/v1.16/docs)を日本語訳したものです。***

![logo](../../img/pulsar.png)

Pulsarは、非常に柔軟なメッセージングモデルと直感的なクライアントAPIを備えた分散pub-subメッセージングプラットフォームです。

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.pulsar/pulsar/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.pulsar/pulsar)


## 主な特徴
* 水平方向にスケーラブル (数百万の独立したトピックと毎秒数百万発行されるメッセージ) 
* 強い順序と一貫性の保証
* 低レイテンシな永続性のあるストレージ
* トピックとキューのセマンティクス
* ロードバランサ
* ホステッドサービスとしてデプロイ可能な設計:
  * マルチテナント
  * 認証
  * 認可
  * リソース割り当て
  * 著しく異なるワークロードのサポート
  * 選択的なハードウェアの分離
* Consumerのカーソル位置を常に把握
* 提供準備、管理、統計のためのREST API
* ジオレプリケーション
* 透過的なパーティションドトピックの処理
* 透過的なメッセージのバッチ処理

## ドキュメンテーション

* [Pulsar入門](GettingStarted.md)
* [システム概要](Architecture.md)
* [ドキュメンテーションのインデックス](Documentation.md)
* [Yahoo Eng Blogのアナウンス記事](https://yahooeng.tumblr.com/post/150078336821/open-sourcing-pulsar-pub-sub-messaging-at-scale)

## 連絡先
* 開発に関する議論は [Pulsar-Dev](https://groups.google.com/d/forum/pulsar-dev) へ
* ユーザからの質問は [Pulsar-Users](https://groups.google.com/d/forum/pulsar-users) へ

## ライセンス

Copyright 2016 Yahoo Inc.

Apache License Version 2.0に基づいて使用許諾されます: http://www.apache.org/licenses/LICENSE-2.0
