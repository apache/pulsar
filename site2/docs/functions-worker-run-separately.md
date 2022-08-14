---
id: functions-worker-run-separately
title: Run function workers separately
sidebar_label: "Run function workers separately"
---

The following diagram illustrates how function workers run as a separate process in separate machines.

![assets/functions-worker-separated.svg](/assets/function-workers-separated.svg)

:::note  

The `Service URLs` in the illustration represent Pulsar service URLs that Pulsar client and Pulsar admin use to connect to a Pulsar cluster.

:::

To set up function workers that run separately, complete the following steps:
1. [Configure function workers](#configure-function-workers-to-run-separately)
2. [Start function workers](#start-function-workers)
3. [Configure proxies for function workers](#configure-proxies-for-standalone-function-workers)

## Configure function workers to run separately

:::note

To run function workers separately, you need to keep `functionsWorkerEnabled` as its default value (`false`) in the `conf/broker.conf` file.

:::

### Configure worker parameters

Configure the required parameters for workers in the `conf/functions_worker.yml` file.
- `workerId`: The identity of a worker node, which is unique across clusters. The type is string.
- `workerHostname`: The hostname of the worker node.
- `workerPort`: The port that the worker server listens on. Keep it as default if you don't customize it. Set it to `null` to disable the plaintext port.
- `workerPortTls`: The TLS port that the worker server listens on. Keep it as default if you don't customize it. For more information about TLS encryption settings, refer to [settings](#enable-tls-encryption).

:::note

When accessing function workers to manage functions, the `pulsar-admin` CLI or any of the clients should use the configured `workerHostname` and `workerPort` to generate an `--admin-url`.

:::

### Configure function package parameters

Configure the `numFunctionPackageReplicas` parameter in the `conf/functions_worker.yml` file. It indicates the number of replicas to store function packages. 

:::note

To ensure high availability in a production deployment, set `numFunctionPackageReplicas` to equal the number of bookies. The default value `1` is only for one-node cluster deployment. 

:::

### Configure function metadata parameters

Configure the required parameter for function metadata in the `conf/functions_worker.yml` file.
- `pulsarServiceUrl`: The Pulsar service URL for your broker cluster.
- `pulsarWebServiceUrl`: The Pulsar web service URL for your broker cluster.
- `pulsarFunctionsCluster`: Set the value to your Pulsar cluster name (same as the `clusterName` setting in the `conf/broker.conf` file).

If authentication is enabled on your broker cluster, you must configure the following authentication settings for the function workers to communicate with the brokers.
- `brokerClientAuthenticationEnabled`: Whether to enable the broker client authentication used by function workers to talk to brokers.
- `clientAuthenticationPlugin`: The authentication plugin to be used by the Pulsar client used in worker service.
- `clientAuthenticationParameters`: The authentication parameter to be used by the Pulsar client used in worker service.

### Enable security settings

When you run a function worker separately in a cluster configured with authentication, your function worker needs to communicate with the broker and authenticate incoming requests. Thus you need to configure the properties that the broker requires for authentication and authorization.

:::note

You must configure both the function worker authentication and authorization for the server to authenticate incoming requests and configure the client to be authenticated to communicate with the broker.

:::

For example, if you use token authentication, you need to configure the following properties in the `conf/function-worker.yml` file.

```yaml

brokerClientAuthenticationPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters: file:///etc/pulsar/token/admin-token.txt
configurationMetadataStoreUrl: zk:zookeeper-cluster:2181 # auth requires a connection to zookeeper
authenticationProviders:
 - "org.apache.pulsar.broker.authentication.AuthenticationProviderToken"
authorizationEnabled: true
authenticationEnabled: true
superUserRoles:
  - superuser
  - proxy
properties:
  tokenSecretKey: file:///etc/pulsar/jwt/secret # if using a secret token, key file must be DER-encoded
  tokenPublicKey: file:///etc/pulsar/jwt/public.key # if using public/private key tokens, key file must be DER-encoded

```

You can enable the following security settings on function workers.
- [Enable TLS encryption](#enable-tls-transport-encryption)
- [Enable authentication providers](#enable-authentication-providers)
- [Enable authorization providers](#enable-authorization-providers)
- [Enable end-to-end encryption](functions-deploy-cluster-encryption.md)


#### Enable TLS encryption

To enable TLS encryption, configure the following settings.

```yaml

useTLS: true
pulsarServiceUrl: pulsar+ssl://localhost:6651/
pulsarWebServiceUrl: https://localhost:8443

tlsEnabled: true
tlsCertificateFilePath: /path/to/functions-worker.cert.pem
tlsKeyFilePath:         /path/to/functions-worker.key-pk8.pem
tlsTrustCertsFilePath:  /path/to/ca.cert.pem

// The path to trusted certificates used by the Pulsar client to authenticate with Pulsar brokers
brokerClientTrustCertsFilePath: /path/to/ca.cert.pem

```

For more details on TLS encryption, refer to [Transport Encryption using TLS](security-tls-transport.md).


#### Enable authentication providers

To enable authentication providers on function workers, substitute the `authenticationProviders` parameter with the providers you want to enable.

```properties

authenticationEnabled: true
authenticationProviders: [provider1, provider2]

```

For [TLS authentication](security-tls-authentication.md) provider, follow the example below to add the required settings.

```properties

brokerClientAuthenticationPlugin: org.apache.pulsar.client.impl.auth.AuthenticationTls
brokerClientAuthenticationParameters: tlsCertFile:/path/to/admin.cert.pem,tlsKeyFile:/path/to/admin.key-pk8.pem

authenticationEnabled: true
authenticationProviders: ['org.apache.pulsar.broker.authentication.AuthenticationProviderTls']

```

For SASL authentication provider, add `saslJaasClientAllowedIds` and `saslJaasServerSectionName` under `properties`. 

```properties

properties:
  saslJaasClientAllowedIds: .*pulsar.*
  saslJaasServerSectionName: Broker

```

For [token authentication](security-jwt.md) provider, add the required settings under `properties`.

```properties

properties:
  tokenSecretKey:       file://my/secret.key 
  # If using public/private
  # tokenPublicKey:     file://path/to/public.key 

```

:::note

Key files must be DER (Distinguished Encoding Rules)-encoded.

:::

#### Enable authorization providers

To enable authorization on function workers, complete the following steps.

1. Configure `authorizationEnabled`, `authorizationProvider` and `configurationMetadataStoreUrl` in the `functions_worker.yml` file. The authentication provider connects to `configurationMetadataStoreUrl` to receive namespace policies.

   ```yaml

   authorizationEnabled: true
   authorizationProvider: org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider
   configurationMetadataStoreUrl: <meta-type>:<configuration-metadata-store-url>

   ```

2. Configure a list of superuser roles. The superuser roles can access any admin API. The following configuration is an example.

   ```yaml

   superUserRoles:
     - role1
     - role2
     - role3

   ```

### Configure BookKeeper authentication

If authentication is enabled on the BookKeeper cluster, you need to configure the following BookKeeper authentication settings for your function workers.
- `bookkeeperClientAuthenticationPlugin`: the authentication plugin name of BookKeeper client.
- `bookkeeperClientAuthenticationParametersName`: the authentication plugin parameters of BookKeeper client, including names and values.
- `bookkeeperClientAuthenticationParameters`: the authentication plugin parameters of BookKeeper client.

## Start function workers

:::note

Before starting function workers, make sure [function runtime](functions-runtime.md) is configured.

:::

* You can start a function worker in the background by using [nohup](https://en.wikipedia.org/wiki/Nohup) with the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) CLI tool:

  ```bash

  bin/pulsar-daemon start functions-worker

  ```

* To start a function worker in the foreground, you can use the [`pulsar-admin`](/tools/pulsar-admin/) CLI as follows.

  ```bash

  bin/pulsar functions-worker

  ```

## Configure proxies for standalone function workers

When you are running function workers in a separate cluster, the admin rest endpoints are split into two clusters as shown in the following figure. The `functions`, `function-worker`, `source`, and `sink` endpoints are now served by the worker cluster, while all the other remaining endpoints are served by the broker cluster. This requires you to use the right service URL accordingly in the `pulsar-admin` CLI. To address this inconvenience, you can start a proxy cluster that serves as the central entry point of the admin service for routing admin rest requests.

![assets/functions-worker-separated-proxy.svg](/assets/function-workers-separated-with-proxy.svg)

:::tip

If you haven't set up a proxy cluster yet, follow the [instructions](administration-proxy.md) to deploy one.

:::

To enable a proxy for routing function-related admin requests to function workers, you can edit the `conf/proxy.conf` file to modify the following settings:

```conf

functionWorkerWebServiceURL=<pulsar-functions-worker-web-service-url>
functionWorkerWebServiceURLTLS=<pulsar-functions-worker-web-service-url>

```
