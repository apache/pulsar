---
id: version-2.8.0-kubernetes-helm
title: Get started in Kubernetes
sidebar_label: Run Pulsar in Kubernetes
original_id: kubernetes-helm
---

This section guides you through every step of installing and running Apache Pulsar with Helm on Kubernetes quickly, including the following sections:

- Install the Apache Pulsar on Kubernetes using Helm
- Start and stop Apache Pulsar
- Create topics using `pulsar-admin`
- Produce and consume messages using Pulsar clients
- Monitor Apache Pulsar status with Prometheus and Grafana

For deploying a Pulsar cluster for production usage, read the documentation on [how to configure and install a Pulsar Helm chart](helm-deploy.md).

## Prerequisite

- Kubernetes server 1.14.0+
- kubectl 1.14.0+
- Helm 3.0+

> #### Tip
> For the following steps, step 2 and step 3 are for **developers** and step 4 and step 5 are for **administrators**.

## Step 0: Prepare a Kubernetes cluster

Before installing a Pulsar Helm chart, you have to create a Kubernetes cluster. You can follow [the instructions](helm-prepare.md) to prepare a Kubernetes cluster.

We use [Minikube](https://minikube.sigs.k8s.io/docs/start/) in this quick start guide. To prepare a Kubernetes cluster, follow these steps:

1. Create a Kubernetes cluster on Minikube.

    ```bash
    minikube start --memory=8192 --cpus=4 --kubernetes-version=<k8s-version>
    ```

    The `<k8s-version>` can be any [Kubernetes version supported by your Minikube installation](https://minikube.sigs.k8s.io/docs/reference/configuration/kubernetes/), such as `v1.16.1`.

2. Set `kubectl` to use Minikube.

    ```bash
    kubectl config use-context minikube
    ```

3. To use the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) with the local Kubernetes cluster on Minikube, enter the command below:

    ```bash
    minikube dashboard
    ```
    The command automatically triggers opening a webpage in your browser. 

## Step 1: Install Pulsar Helm chart

0. Add Pulsar charts repo.

    ```bash
    helm repo add apache https://pulsar.apache.org/charts
    ```

    ```bash
    helm repo update
    ```

1. Clone the Pulsar Helm chart repository.

    ```bash
    git clone https://github.com/apache/pulsar-helm-chart
    cd pulsar-helm-chart
    ```

2. Run the script `prepare_helm_release.sh` to create secrets required for installing the Apache Pulsar Helm chart. The username `pulsar` and password `pulsar` are used for logging into the Grafana dashboard and Pulsar Manager.

    ```bash
    ./scripts/pulsar/prepare_helm_release.sh \
        -n pulsar \
        -k pulsar-mini \
        -c
    ```

3. Use the Pulsar Helm chart to install a Pulsar cluster to Kubernetes.

   > **NOTE**  
   > You need to specify `--set initialize=true` when installing Pulsar the first time. This command installs and starts Apache Pulsar.

    ```bash
    helm install \
        --values examples/values-minikube.yaml \
        --set initialize=true \
        --namespace pulsar \
        pulsar-mini apache/pulsar
    ```

4. Check the status of all pods.

    ```bash
    kubectl get pods -n pulsar
    ```

    If all pods start up successfully, you can see that the `STATUS` is changed to `Running` or `Completed`.

    **Output**

    ```bash
    NAME                                         READY   STATUS      RESTARTS   AGE
    pulsar-mini-bookie-0                         1/1     Running     0          9m27s
    pulsar-mini-bookie-init-5gphs                0/1     Completed   0          9m27s
    pulsar-mini-broker-0                         1/1     Running     0          9m27s
    pulsar-mini-grafana-6b7bcc64c7-4tkxd         1/1     Running     0          9m27s
    pulsar-mini-prometheus-5fcf5dd84c-w8mgz      1/1     Running     0          9m27s
    pulsar-mini-proxy-0                          1/1     Running     0          9m27s
    pulsar-mini-pulsar-init-t7cqt                0/1     Completed   0          9m27s
    pulsar-mini-pulsar-manager-9bcbb4d9f-htpcs   1/1     Running     0          9m27s
    pulsar-mini-toolset-0                        1/1     Running     0          9m27s
    pulsar-mini-zookeeper-0                      1/1     Running     0          9m27s
    ```

5. Check the status of all services in the namespace `pulsar`.

    ```bash
    kubectl get services -n pulsar
    ```

    **Output**
    
    ```bash
    NAME                         TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)                       AGE
    pulsar-mini-bookie           ClusterIP      None             <none>        3181/TCP,8000/TCP             11m
    pulsar-mini-broker           ClusterIP      None             <none>        8080/TCP,6650/TCP             11m
    pulsar-mini-grafana          LoadBalancer   10.106.141.246   <pending>     3000:31905/TCP                11m
    pulsar-mini-prometheus       ClusterIP      None             <none>        9090/TCP                      11m
    pulsar-mini-proxy            LoadBalancer   10.97.240.109    <pending>     80:32305/TCP,6650:31816/TCP   11m
    pulsar-mini-pulsar-manager   LoadBalancer   10.103.192.175   <pending>     9527:30190/TCP                11m
    pulsar-mini-toolset          ClusterIP      None             <none>        <none>                        11m
    pulsar-mini-zookeeper        ClusterIP      None             <none>        2888/TCP,3888/TCP,2181/TCP    11m
    ```

## Step 2: Use pulsar-admin to create Pulsar tenants/namespaces/topics

`pulsar-admin` is the CLI (command-Line Interface) tool for Pulsar. In this step, you can use `pulsar-admin` to create resources, including tenants, namespaces, and topics.

1. Enter the `toolset` container.

    ```bash
    kubectl exec -it -n pulsar pulsar-mini-toolset-0 -- /bin/bash
    ```

2. In the `toolset` container, create a tenant named `apache`.

    ```bash
    bin/pulsar-admin tenants create apache
    ```

    Then you can list the tenants to see if the tenant is created successfully.

    ```bash
    bin/pulsar-admin tenants list
    ```

    You should see a similar output as below. The tenant `apache` has been successfully created. 

    ```bash
    "apache"
    "public"
    "pulsar"
    ```

3. In the `toolset` container, create a namespace named `pulsar` in the tenant `apache`.

    ```bash
    bin/pulsar-admin namespaces create apache/pulsar
    ```

    Then you can list the namespaces of tenant `apache` to see if the namespace is created successfully.

    ```bash
    bin/pulsar-admin namespaces list apache
    ```

    You should see a similar output as below. The namespace `apache/pulsar` has been successfully created. 

    ```bash
    "apache/pulsar"
    ```

4. In the `toolset` container, create a topic `test-topic` with `4` partitions in the namespace `apache/pulsar`.

    ```bash
    bin/pulsar-admin topics create-partitioned-topic apache/pulsar/test-topic -p 4
    ```

5. In the `toolset` container, list all the partitioned topics in the namespace `apache/pulsar`.

    ```bash
    bin/pulsar-admin topics list-partitioned-topics apache/pulsar
    ```

    Then you can see all the partitioned topics in the namespace `apache/pulsar`.

    ```bash
    "persistent://apache/pulsar/test-topic"
    ```

## Step 3: Use Pulsar client to produce and consume messages

You can use the Pulsar client to create producers and consumers to produce and consume messages.

By default, the Pulsar Helm chart exposes the Pulsar cluster through a Kubernetes `LoadBalancer`. In Minikube, you can use the following command to check the proxy service.

```bash
kubectl get services -n pulsar | grep pulsar-mini-proxy
```

You will see a similar output as below.

```bash
pulsar-mini-proxy            LoadBalancer   10.97.240.109    <pending>     80:32305/TCP,6650:31816/TCP   28m
```

This output tells what are the node ports that Pulsar cluster's binary port and HTTP port are mapped to. The port after `80:` is the HTTP port while the port after `6650:` is the binary port.

Then you can find the IP address and exposed ports of your Minikube server by running the following command.

```bash
minikube service pulsar-mini-proxy -n pulsar
```

**Output**

```bash
|-----------|-------------------|-------------|-------------------------|
| NAMESPACE |       NAME        | TARGET PORT |           URL           |
|-----------|-------------------|-------------|-------------------------|
| pulsar    | pulsar-mini-proxy | http/80     | http://172.17.0.4:32305 |
|           |                   | pulsar/6650 | http://172.17.0.4:31816 |
|-----------|-------------------|-------------|-------------------------|
🏃  Starting tunnel for service pulsar-mini-proxy.
|-----------|-------------------|-------------|------------------------|
| NAMESPACE |       NAME        | TARGET PORT |          URL           |
|-----------|-------------------|-------------|------------------------|
| pulsar    | pulsar-mini-proxy |             | http://127.0.0.1:61853 |
|           |                   |             | http://127.0.0.1:61854 |
|-----------|-------------------|-------------|------------------------|
```

At this point, you can get the service URLs to connect to your Pulsar client. Here are URL examples:
```
webServiceUrl=http://127.0.0.1:61853/
brokerServiceUrl=pulsar://127.0.0.1:61854/
```

Then you can proceed with the following steps:

1. Download the Apache Pulsar tarball from the [downloads page](https://pulsar.apache.org/en/download/).

2. Decompress the tarball based on your download file.

    ```bash
    tar -xf <file-name>.tar.gz
    ```

3. Expose `PULSAR_HOME`.

    (1) Enter the directory of the decompressed download file.

    (2) Expose `PULSAR_HOME` as the environment variable.

    ```bash
    export PULSAR_HOME=$(pwd)
    ```

4. Configure the Pulsar client.

    In the `${PULSAR_HOME}/conf/client.conf` file, replace `webServiceUrl` and `brokerServiceUrl` with the service URLs you get from the above steps.

5. Create a subscription to consume messages from `apache/pulsar/test-topic`.

    ```bash
    bin/pulsar-client consume -s sub apache/pulsar/test-topic  -n 0
    ```

6. Open a new terminal. In the new terminal, create a producer and send 10 messages to the `test-topic` topic.

    ```bash
    bin/pulsar-client produce apache/pulsar/test-topic  -m "---------hello apache pulsar-------" -n 10
    ```

7. Verify the results.

    - From the producer side

        **Output**
        
        The messages have been produced successfully.

        ```bash
        18:15:15.489 [main] INFO  org.apache.pulsar.client.cli.PulsarClientTool - 10 messages successfully produced
        ```

    - From the consumer side

        **Output**

        At the same time, you can receive the messages as below.

        ```bash
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ----- got message -----
        ---------hello apache pulsar-------
        ```

## Step 4: Use Pulsar Manager to manage the cluster

[Pulsar Manager](administration-pulsar-manager.md) is a web-based GUI management tool for managing and monitoring Pulsar.

1. By default, the `Pulsar Manager` is exposed as a separate `LoadBalancer`. You can open the Pulsar Manager UI using the following command:

    ```bash
    minikube service -n pulsar pulsar-mini-pulsar-manager 
    ```

2. The Pulsar Manager UI will be open in your browser. You can use the username `pulsar` and password `pulsar` to log into Pulsar Manager.

3. In Pulsar Manager UI, you can create an environment. 

    - Click `New Environment` button in the top-left corner.
    - Type `pulsar-mini` for the field `Environment Name` in the popup window.
    - Type `http://pulsar-mini-broker:8080` for the field `Service URL` in the popup window.
    - Click `Confirm` button in the popup window.

4. After successfully created an environment, you are redirected to the `tenants` page of that environment. Then you can create `tenants`, `namespaces` and `topics` using the Pulsar Manager.

## Step 5: Use Prometheus and Grafana to monitor cluster

Grafana is an open-source visualization tool, which can be used for visualizing time series data into dashboards.

1. By default, the Grafana is exposed as a separate `LoadBalancer`. You can open the Grafana UI using the following command:

    ```bash
    minikube service pulsar-mini-grafana -n pulsar
    ```

2. The Grafana UI is open in your browser. You can use the username `pulsar` and password `pulsar` to log into the Grafana Dashboard.

3. You can view dashboards for different components of a Pulsar cluster.
