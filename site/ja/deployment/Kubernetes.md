---
title: Kubernetes上でのPulsarのデプロイ
tags_ja: [Kubernetes, Google Kubernetes Engine]
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

Pulsarは[Google Kubernetes Engine](#google-kubernetes-engine), [Amazon Web Services](https://aws.amazon.com/)の管理クラスタ、[カスタムクラスタ](#カスタムkubernetesクラスタ)の[Kubernetes](https://kubernetes.io/)クラスタに簡単にデプロイできます。

このガイドのデプロイ方法はKubernetesの[リソース](https://kubernetes.io/docs/resources-reference/v1.6/)の[YAML](http://yaml.org/)定義に依存しています。[Pulsarパッケージ](/download)の[`kubernetes`]({{ site.pulsar_repo }}/kubernetes)サブディレクトリは以下のリソース定義を保持します:

* 2つの{% popover_ja Bookie %}を持つ{% popover_ja BookKeeper %}クラスタ
* 3つのノードを持つ{% popover_ja ZooKeeper %}クラスタ
* 3つの{% popover_ja Broker %}を持つPulsar{% popover_ja クラスタ %}
* [Prometheus](https://prometheus.io/), [Grafana](https://grafana.com), [Pulsarダッシュボード](../../admin/Dashboard)からなる監視スタック
* CLIツール[`pulsar-admin`](../../reference/CliTools#pulsar-admin)を用いて管理コマンドを実行できる[Pod](https://kubernetes.io/docs/concepts/workloads/pods/pod/)

## セットアップ

まず、[ダウンロードページ](/download)からソースパッケージをインストールしてください。

{% include admonition.html type='warning' content="PulsarのバイナリパッケージにはPulsarをKubernetes上にデプロイするために必要なYAMLリソースは*含まれない*ことに注意してください。" %}

もしPulsarクラスタのBookie, Broker, ZooKeeperノードの数を変更したい場合は、適切な[`Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), [`StatefulSet`](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)リソースの`spec`セクション内の`replicas`パラメータを編集してください。

## Google Kubernetes Engine

[Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine) (GKE) は[Google Compute Engine](https://cloud.google.com/compute/) (GCE) 内のKubernetesクラスタの作成、管理を自動化します。

### 前提条件

前提条件として以下が必要です:

* [cloud.google.com](https://cloud.google.com)にサインアップできるGoogle Cloud Platformアカウント
* 既存のCloud Platformプロジェクト
* [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (特に[`gcloud`](https://cloud.google.com/sdk/gcloud/)と`kubectl`ツール)

### 新しいKubernetesクラスタの作成

`gcloud`の[`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create)コマンドを使って、GKEクラスタを作成できます。このコマンドによってクラスタのノード数、ノードのマシンタイプなどを指定できます。

例として、[us-central1-a](https://cloud.google.com/compute/docs/regions-zones/regions-zones#available)ゾーンにKubernetesバージョン[1.6.4](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG.md#v164)のGKEクラスタを作成します。クラスタは`pulsar-gke-cluster`と命名し、2つローカルSSDを備えた3つのVMを[n1-standard-8](https://cloud.google.com/compute/docs/machine-types)というマシンタイプで起動します。これらのSSDは{% popover_ja Bookie %}インスタンスに使用され、1つはBookKeeperの[Journal](../../getting-started/ConceptsAndArchitecture#journalストレージ)、もう1つは実際のメッセージデータが保存されます。

```bash
$ gcloud container clusters create pulsar-gke-cluster \
  --zone=us-central1-a \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
  --cluster-version=1.6.4
```

デフォルトでは、BookieはSSDを備えた全てのマシンで起動します。この例では、全てのマシンが2つのSSDを持ちますが異なるタイプのマシンを後からクラスタに追加することもできます。どのマシンをBookieサーバにするかは[Label](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels)を使用してコントロールできます。

### ダッシュボード

KubernetesクラスタのCredentialをダウンロードし、クラスタに対するプロキシをオープンすることによって、[Kubernetesダッシュボード](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/)でクラスタを監視できます:

```bash
$ gcloud container clusters get-credentials pulsar-gke-cluster \
  --zone=us-central1-a \
  --project=your-project-name
$ kubectl proxy
```

デフォルトでは、プロキシは8001番ポートでオープンされます。ブラウザから[localhost:8001/ui](http://localhost:8001/ui)にアクセスすることで、ダッシュボードを利用できます。最初、GKEクラスタは空ですが、Pulsar[コンポーネント](#Pulsarコンポーネントのデプロイ)をデプロイすることによって変更されます。

## Amazon Web Services

[Amazon Web Services](https://aws.amazon.com/) (AWS) では様々な方法でKubernetesを利用できます。[最近紹介された](https://aws.amazon.com/blogs/compute/kubernetes-clusters-aws-kops/)非常にシンプルな方法は、[Kubernetes Operations](https://github.com/kubernetes/kops) (kops) ツールを利用するものです。

AWSでKubernetesクラスタをセットアップするための詳細な説明は[こちら](https://github.com/kubernetes/kops/blob/master/docs/aws.md)です。

この説明を用いてクラスタを作成した時、`~/.kube/config` (MacOS, Linuxの場合) にある`kubectl`の設定ファイルは、恐らく変更しなくても良いようにアップデートされます。クラスタ内のノードのリストを表示する事によって、`kubectl`がクラスタに接続できるかを確認できます:

```bash
$ kubectl get nodes
```

`kubectl`がクラスタで機能していれば、[Pulsarコンポーネントのデプロイ](#Pulsarコンポーネントのデプロイ)に進むことができます。

## カスタムKubernetesクラスタ

Pulsarは独自のGKEではないKubernetesクラスタに対してもデプロイできます。要件を満たすKubernetesのインストール方法の選択方法についての詳細なドキュメントは、Kubernetesドキュメントの[適切なソリューションの選択](https://kubernetes.io/docs/setup/pick-right-solution)をご確認ください。

### ローカルクラスタ

Kubernetesクラスタを立ち上げる最も簡単な方法は、ローカルで行うことです。ローカルVMでテスト目的で小さなローカルクラスタをインストールするために、次のいずれかを選択できます:

1. シングルノードのKubernetesクラスタを立ち上げるために[minikube](https://kubernetes.io/docs/getting-started-guides/minikube/)を使う
1. 同じマシン上で複数のVMを立ち上げてローカルクラスタを作成する

2つ目の選択肢については、[Vagrant](https://www.vagrantup.com/)上の[CoreOS](https://coreos.com/)を使ったKubernetesの立ち上げの[説明](https://github.com/pires/kubernetes-vagrant-coreos-cluster)を見てください。ここではこの説明の概要を示します。


まず、[Vagrant](https://www.vagrantup.com/downloads.html)と[VirtualBox](https://www.virtualbox.org/wiki/Downloads)がインストールされていることを確認してください。そして、リポジトリをクローンしクラスタをスタートしてください。

```bash
$ git clone https://github.com/pires/kubernetes-vagrant-coreos-cluster
$ cd kubernetes-vagrant-coreos-cluster

# 3つのVMのクラスタを起動
$ NODES=3 USE_KUBE_UI=true vagrant up
```

Create SSD disk mount points on the VMs using this script:

```bash
$ for vm in node-01 node-02 node-03; do
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd0"
    NODES=3 vagrant ssh $vm -c "sudo mkdir -p /mnt/disks/ssd1"
  done
```

{% popover_ja Bookie %}には[Journal](../../getting-started/ConceptsAndArchitecture#journalストレージ)と永続メッセージストレージのために、2つの論理デバイスがマウントされることが想定されています。このVMの例では、それぞれのVMに2つのディレクトリを作成します。

クラスタが起動したら、`kubectl`がアクセスできるか確認できます:

```bash
$ kubectl get nodes
NAME           STATUS                     AGE       VERSION
172.17.8.101   Ready,SchedulingDisabled   10m       v1.6.4
172.17.8.102   Ready                      8m        v1.6.4
172.17.8.103   Ready                      6m        v1.6.4
172.17.8.104   Ready                      4m        v1.6.4
```

### ダッシュボード

まず、ローカルのKubernetesクラスタで[Kubernetesダッシュボード](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/)を使うために、`kubectl`でクラスタに対するプロキシを作成してください:

```bash
$ kubectl proxy
```

ウェブインターフェースで[localhost:8001/ui](http://localhost:8001/ui)にアクセスできます。最初ローカルクラスタは空ですが、Pulsar[コンポーネント](#Pulsarコンポーネントのデプロイ)をデプロイすることによって変更されます。

## Pulsarコンポーネントのデプロイ

ここまでで、[Google Kubernetes Engine](#google-kubernetes-engine)あるいは[カスタムクラスタ](#カスタムkubernetesクラスタ)のいずれかで、Kubernetesクラスタのセットアップが完了しました。Pulsarを構成するコンポーネントをデプロイすることができます。PulsarのコンポーネントのYAMLのリソース定義は[Pulsarソースパッケージ](/download)の`kubernetes`ディレクトリにあります。

このパッケージでは、2セットのリソース定義があります。1つは`kubernetes/google-container-engine`ディレクトリに含まれる、Google Kubernetes Engine (GKE) のためのもの、もう1つは`kubernetes/generic`ディレクトリに含まれるカスタムKubernetesクラスタのためのものです。まず、`cd`で適切なディレクトリへ移動しましょう。

### ZooKeeper

他のコンポーネントが依存するため、{% popover_ja ZooKeeper %}は*必ず*最初にデプロイしてください。

```bash
$ kubectl apply -f zookeeper.yaml
```

3つのZooKeeperサーバのPodがアップし、ステータスが`Running`になるまでお待ちください。ZooKeeperのPodのステータスはいつでも確認できます:

```bash
$ kubectl get pods -l component=zookeeper
NAME      READY     STATUS             RESTARTS   AGE
zk-0      1/1       Running            0          18m
zk-1      1/1       Running            0          17m
zk-2      0/1       Running            6          15m
```

KubernetesがVMにDockerイメージをダウンロードする必要があるため、このステップは数分間かかります。

#### クラスタメタデータの初期化

Zookeeperが起動したら、Pulsarクラスタの[メタデータを初期化](../InstanceSetup#クラスタメタデータの初期化)する必要があります。これは{% popover_ja BookKeeper %}とPulsarのシステムメタデータが広く含まれています。

```bash
$ kubectl exec -it zk-0 -- \
    bin/pulsar initialize-cluster-metadata \
      --cluster us-central \
      --zookeeper zookeeper \
      --configuration-store zookeeper \
      --web-service-url http://broker.default.svc.cluster.local:8080/ \
      --broker-service-url pulsar://broker.default.svc.cluster.local:6650/
```

必要に応じて、クラスタのメタデータ値を修正してください。

#### 残りのコンポーネントのデプロイ

クラスタメタデータが正常に初期化されたら、{% popover_ja Bookie %}, {% popover_ja Broker %}, 監視スタック ([Prometheus](https://prometheus.io), [Grafana](https://grafana.com), [Pulsarダッシュボード](../../admin/Dashboard)) をデプロイできます。

```bash
$ kubectl apply -f bookie.yaml
$ kubectl apply -f broker.yaml
$ kubectl apply -f monitoring.yaml
```

これらのコンポーネントのPodのステータスをKubernetesダッシュボードか`kubectl`のいずれかで確認できます:

```bash
$ kubectl get pods
```

#### プロパティとネームスペースのセットアップ

全てのコンポーネントが起動したら、少なくとも1つの{% popover_ja プロパティ %}と少なくとも1つの{% popover_ja ネームスペース %}を作成する必要があります。

{% include admonition.html type='info' content='
[認証と認可](../../admin/Authz)が有効になっている場合、このステップは必ずしも必要ではありませんが、各ネームスペースの[ポリシー](../../admin/PropertiesNamespaces#ネームスペースの管理)を後から変更できます。
' %}

プロパティとネームスペースを作成するために、新しく作成したPulsarクラスタのクライアントとして機能する`pulsar-admin`のPodに接続してください。

```bash
$ kubectl exec pulsar-admin -it -- bash
```

ここで全ての管理コマンドを実行できます。以下は`prop`という名前のプロパティとそのプロパティ下の`prop/us-central/ns`という名前のネームスペースを作成するコマンドの例です。

```bash
export MY_PROPERTY=prop
export MY_NAMESPACE=prop/us-central/ns

# 新しいPulsarプロパティの設定
$ bin/pulsar-admin properties create $MY_PROPERTY \
  --admin-roles admin \
  --allowed-clusters us-central

# 4のBrokerを横断しうるネームスペースの作成
$ bin/pulsar-admin namespaces create $MY_NAMESPACE
```

#### 作成したクラスタでの実験

プロパティとネームスペースが作成されたので、起動したPulsarクラスタで実験を始められます。例えば、これまでと同じ`pulsar-admin` Podから[`pulsar-perf`](../../reference/CliTools#pulsar-perf)コマンドを使って、作成した{% popover_ja プロパティ %}と{% popover_ja ネームスペース %}内のトピックに秒間10,000のメッセージを発行するテスト{% popover_ja Producer %}を作成できます:

```bash
$ bin/pulsar-perf produce persistent://prop/us-central/ns/my-topic \
  --rate 10000
```

同様に、トピックを購読して全てのメッセージを受信するための{% popover_ja Consumer %}を起動できます:

```bash
$ bin/pulsar-perf consume persistent://prop/us-central/ns/my-topic \
  --subscriber-name my-subscription-name
```

[`pulsar-admin`](../../reference/CliTools#pulsar-admin-persistent-stats)ツールを使って、トピックに対する[統計情報](../../admin/Stats)を閲覧することもできます:

```bash
$ bin/pulsar-admin persistent stats persistent://prop/us-central/ns/my-topic
```

### 監視

Kubernetes上のPulsarのデフォルトの監視スタックは[Prometheus](#prometheus), [Grafana](#grafana), [Pulsarダッシュボード](../../admin/Dashboard)で構成されます。

#### Prometheus

Kubernetes内の全てのPulsarのメトリクスはクラスタ内部で起動している[Prometheus](https://prometheus.io)インスタンスで収集されます。通常、Prometheusに直接アクセスする必要はありません。代わりに、Prometheusに保存されたデータを表示する[Grafanaインターフェース](#grafana)を使用できます。

#### Grafana

Kubernetesクラスタ内で、Pulsarの{% popover_ja ネームスペース %} (メッセージレート、レイテンシ、ストレージ)、JVMの統計情報、 {% popover_ja ZooKeeper %}、{% popover_ja BookKeeper %}のためのダッシュボードを閲覧するために[Grafana](https://grafana.com)を使用できます。`kubectl`の[`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster)コマンドを使ってGrafanaを提供するPodへのアクセス権を得られます:

```bash
$ kubectl port-forward $(kubectl get pods | grep grafana | awk '{print $1}') 3000
```

[localhost:3000](http://localhost:3000)でウェブブラウザからダッシュボードにアクセスできます。

#### Pulsarダッシュボード

GrafanaとPrometheusはヒストリカルデータのグラフの提供に使用されますが、[Pulsarダッシュボード](../../admin/Dashboard)は各{% popover_ja トピック %}に対する現在のデータの詳細をレポートします。

例えば、全てのネームスペース、トピック、Brokerの統計情報のソート可能なテーブル、ConsumerのIPアドレスや接続時間などの詳細なデータが利用可能です。

`kubectl`の[`port-forward`](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster)コマンドを使ってPulsarダッシュボードを提供するPodへのアクセス権を得られます:

```bash
$ kubectl port-forward $(kubectl get pods | grep pulsar-dashboard | awk '{print $1}') 8080:80
```

[localhost:8080](http://localhost:8080)でウェブブラウザからPulsarダッシュボードにアクセスできます。
