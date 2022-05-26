---
id: security-basic-auth
title: Authentication using HTTP basic
sidebar_label: "Authentication using HTTP basic"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

[Basic authentication](https://en.wikipedia.org/wiki/Basic_access_authentication) is a simple authentication scheme built into the HTTP protocol, which uses base64-encoded username and password pairs as credentials.

## Prerequisites

Install [`htpasswd`](https://httpd.apache.org/docs/2.4/programs/htpasswd.html) in your environment to create a password file for storing username-password pairs.

* For Ubuntu/Debian, run the following command to install `htpasswd`.
   
   ```
   apt install apache2-utils
   ```
 
* For CentOS/RHEL, run the following command to install `htpasswd`.

   ```
   yum install httpd-tools
   ```

## Create your authentication file

:::note
Currently, you can use MD5 (recommended) and CRYPT encryption to authenticate your password.
:::

Create a password file named `.htpasswd` with a user account `superuser/admin`:
* Use MD5 encryption (recommended):

   ```
   htpasswd -cmb /path/to/.htpasswd superuser admin
   ```

* Use CRYPT encryption:

   ```
   htpasswd -cdb /path/to/.htpasswd superuser admin
   ```

You can preview the content of your password file by running the following command:

```
cat path/to/.htpasswd
superuser:$apr1$GBIYZYFZ$MzLcPrvoUky16mLcK6UtX/
```

## Enable basic authentication on brokers

To configure brokers to authenticate clients, complete the following steps.

1. Add the following parameters to the `conf/broker.conf` file. If you use a standalone Pulsar, you need to add these parameters to the `conf/standalone.conf` file.

   ```
   # Configuration to enable Basic authentication
   authenticationEnabled=true
   authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderBasic

   # Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters
   brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationBasic
   brokerClientAuthenticationParameters={"userId":"superuser","password":"admin"}

   # If this flag is set then the broker authenticates the original Auth data
   # else it just accepts the originalPrincipal and authorizes it (if required).
   authenticateOriginalAuthData=true
   ```

2. Set an environment variable named `PULSAR_EXTRA_OPTS` and the value is `-Dpulsar.auth.basic.conf=/path/to/.htpasswd`. Pulsar reads this environment variable to implement HTTP basic authentication.

## Enable basic authentication on proxies

To configure proxies to authenticate clients, complete the following steps.

1. Add the following parameters to the `conf/proxy.conf` file:

   ```
   # For clients connecting to the proxy
   authenticationEnabled=true
   authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderBasic

   # For the proxy to connect to brokers
   brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationBasic
   brokerClientAuthenticationParameters={"userId":"superuser","password":"admin"}

   # Whether client authorization credentials are forwarded to the broker for re-authorization.
   # Authentication must be enabled via authenticationEnabled=true for this to take effect.
   forwardAuthorizationCredentials=true
   ```

2. Set an environment variable named `PULSAR_EXTRA_OPTS` and the value is `-Dpulsar.auth.basic.conf=/path/to/.htpasswd`. Pulsar reads this environment variable to implement HTTP basic authentication.

## Configure basic authentication in CLI tools

[Command-line tools](/docs/next/reference-cli-tools), such as [Pulsar-admin](/tools/pulsar-admin/), [Pulsar-perf](/tools/pulsar-perf/) and [Pulsar-client](/tools/pulsar-client/), use the `conf/client.conf` file in your Pulsar installation. To configure basic authentication in Pulsar CLI tools, you need to add the following parameters to the `conf/client.conf` file.

```
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationBasic
authParams={"userId":"superuser","password":"admin"}
```


## Configure basic authentication in Pulsar clients

The following example shows how to configure basic authentication when using Pulsar clients.

<Tabs>
  <TabItem value="Java" label="Java" default>

   ```java
   AuthenticationBasic auth = new AuthenticationBasic();
   auth.configure("{\"userId\":\"superuser\",\"password\":\"admin\"}");
   PulsarClient client = PulsarClient.builder()
      .serviceUrl("pulsar://broker.example.com:6650")
      .authentication(auth)
      .build();
   ```

  </TabItem>
</Tabs>
