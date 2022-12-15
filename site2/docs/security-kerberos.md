---
id: security-kerberos
title: Authentication using Kerberos
sidebar_label: "Authentication using Kerberos"
---

[Kerberos](https://web.mit.edu/kerberos/) is a network authentication protocol designed to provide strong authentication for client applications and server applications by using secret-key cryptography. 

In Pulsar, you can use Kerberos with [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer) as a choice for authentication. Since Pulsar uses the [Java Authentication and Authorization Service (JAAS)](https://en.wikipedia.org/wiki/Java_Authentication_and_Authorization_Service) for SASL configuration, you need to provide JAAS configurations for Kerberos authentication. 

:::note

Kerberos authentication uses the authenticated principal as the role token for [Pulsar authorization](security-authorization.md). If you've enabled `authorizationEnabled`, you need to set `superUserRoles` in `broker.conf` that corresponds to the name registered in KDC. For example:

```bash
superUserRoles=client/{clientIp}@EXAMPLE.COM
```

:::

## Prerequisites

- Set up and run a [Key Distribution Center(KDC)](https://en.wikipedia.org/wiki/Key_distribution_center).
- Install a Kerberos server if your organization doesn't have one. Your Linux vendor might have packages for `Kerberos`. For how to install and configure Kerberos, see [Ubuntu](https://help.ubuntu.com/community/Kerberos) and 
[Redhat](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html).
- If you use Oracle Java, you need to download JCE policy files for your Java version and copy them to the `$JAVA_HOME/jre/lib/security` directory.

## Enable Kerberos authentication on brokers

### Create Kerberos principals

If you use the existing Kerberos system, ask your Kerberos administrator to obtain a principal for each broker in your cluster and for every operating system user that accesses Pulsar with Kerberos authentication (via clients and CLI tools).

If you have installed your own Kerberos system, you need to create these principals with the following commands:

```shell
### add Principals for broker
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}"
### add Principals for client
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}"
```

The first part of broker principal (for example, `broker` in `broker/{hostname}@{REALM}`) is the `serverType` of each host. The suggested values of `serverType` are `broker` (host machine runs Pulsar broker service) and `proxy` (host machine runs Pulsar Proxy service). 

Note that *Kerberos* requires that all your hosts can be resolved with their FQDNs.

### Configure brokers
 
In the `broker.conf` file, set Kerberos-related configurations. Here is an example:

```conf
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.* ## regex for principals that are allowed to connect to brokers
saslJaasServerSectionName=PulsarBroker ## corresponds to the section in the JAAS configuration file for brokers

# Authentication settings of the broker itself. Used when the broker connects to other brokers, or when the proxy connects to brokers, either in same or other clusters
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
brokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}
```

To make Pulsar internal admin client work properly, you need to: 
- Set `brokerClientAuthenticationPlugin` to client plugin `AuthenticationSasl`;
- Set `brokerClientAuthenticationParameters` to value in JSON string `{"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}`, in which `PulsarClient` is the section name in the `pulsar_jaas.conf` file, and `"serverType":"broker"` indicates that the internal admin client connects to a broker.

### Configure JAAS

JAAS configuration file provides the information to connect KDC. Here is an example named `pulsar_jaas.conf`:

```conf
 PulsarBroker {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarbroker.keytab"
   principal="broker/localhost@EXAMPLE.COM";
};

 PulsarClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarclient.keytab"
   principal="client/localhost@EXAMPLE.COM";
};
```

In the above example:
- `PulsarBroker` is a section name in the JAAS file that each broker uses. This section tells the broker to use which principal inside Kerberos and the location of the keytab where the principal is stored. 
- `PulsarClient` is a section name in the JASS file that each client uses. This section tells the client to use which principal inside Kerberos and the location of the keytab where the principal is stored.

You need to set the `pulsar_jaas.conf` file path as a JVM parameter. For example:

```shell
    -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf
```

### Connect to KDC

:::note

If your machines configured with Kerberos already have a system-wide configuration, you can skip this configuration.

:::

The content of `krb5.conf` file indicates the default Realm and KDC information. See [JDK’s Kerberos Requirements](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details.

To specify the path to the `krb5.conf` file for brokers, enter the command below. 

```shell
-Djava.security.krb5.conf=/etc/pulsar/krb5.conf
```

Here is an example of the `krb5.conf` file. 

```conf
[libdefaults]
 default_realm = EXAMPLE.COM

[realms]
 EXAMPLE.COM  = {
  kdc = localhost:62037
 }
```

In the above example:
- `EXAMPLE.COM` is the default Realm;
- `kdc = localhost:62037` is the KDC server URL for the `EXAMPLE.COM` Realm.

## Enable Kerberos authentication on proxies

If you want to use proxies between brokers and clients, Pulsar proxies (as a SASL server in Kerberos) will authenticate clients (as a SASL client in Kerberos) before brokers authenticate proxies. 

### Create Kerberos principals

Add new principals for Pulsar proxies.

```shell
### add Principals for Pulsar Proxy
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey proxy/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{proxy-keytabname}.keytab proxy/{hostname}@{REALM}"
```

For principals set for brokers and clients, see [here](#create-kerberos-principals).

### Configure proxies

In the `proxy.conf` file, set Kerberos-related configuration. 

```shell
## related to authenticate client.
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasServerSectionName=PulsarProxy

## related to be authenticated by broker
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
brokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarProxy", "serverType":"broker"}
forwardAuthorizationCredentials=true
```

In the above example:
- The first part relates to the authentication between clients and proxies. In this phase, clients work as SASL clients, while proxies work as SASL servers. 
- The second part relates to the authentication between proxies and brokers. In this phase, proxies work as SASL clients, while brokers work as SASL servers.

### Configure JAAS

Add a new section for proxies in the `pulsar_jaas.conf` file. Here is an example:

```conf
 PulsarProxy {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarproxy.keytab"
   principal="proxy/localhost@EXAMPLE.COM";
};
```

## Configure Kerberos authentication in Java clients

:::note

Ensure that the operating system user who starts Pulsar clients can access the keytabs configured in the `pulsar_jaas.conf` file and the KDC server configured in the `krb5.conf` file.

:::

1. In client applications, include `pulsar-client-auth-sasl` in your project dependency.

   ```xml
       <dependency>
         <groupId>org.apache.pulsar</groupId>
         <artifactId>pulsar-client-auth-sasl</artifactId>
         <version>${pulsar.version}</version>
       </dependency>
   ```

2. Configure the authentication type to use `AuthenticationSasl` and provide the following parameters. 
   - set `saslJaasClientSectionName` to `PulsarClient`;
   - set `serverType` to `broker`. `serverType` stands for whether this client connects to brokers or proxies. Clients use this parameter to know which server-side principal should be used. 

   The following is an example of configuring a Java client:

    ```java
    System.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");
    System.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");

    Map<String, String> authParams = Maps.newHashMap();
    authParams.put("saslJaasClientSectionName", "PulsarClient");
    authParams.put("serverType", "broker");

    Authentication saslAuth = AuthenticationFactory
            .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);
 
    PulsarClient client = PulsarClient.builder()
            .serviceUrl("pulsar://my-broker.com:6650")
            .authentication(saslAuth)
            .build();
    ```

   :::note
   
   - To configure clients for proxies, you need to set `serverType` to `proxy` instead of `broker`.
   - The first two lines in the above example are hard-coded. Alternatively, you can set additional JVM parameters for `pulsar_jaas.conf` and `krb5.conf` files when you run the application like below:

   ```shell
   java -cp -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf $APP-jar-with-dependencies.jar $CLASSNAME
   ```

   :::

## Configure Kerberos authentication in CLI tools

[Command-line tools](reference-cli-tools.md) like [`pulsar-admin`](/tools/pulsar-admin/), [`pulsar-perf`](reference-cli-tools.md#pulsar-perf), and [`pulsar-client`](reference-cli-tools.md#pulsar-client) use the `conf/client.conf` file in a Pulsar installation.

When using command-line tools, you need to perform the following steps:

1. Configure the `conf/client.conf` file.

   ```shell
   authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
   authParams={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}
   ```

2. Set JVM parameters for the `pulsar_jaas.conf` file and `krb5.conf` files with additional options.

   ```shell
   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf
   ```

   You can add this at the end of `PULSAR_EXTRA_OPTS` in the file [`pulsar_tools_env.sh`](https://github.com/apache/pulsar/blob/master/conf/pulsar_tools_env.sh), or add this line `OPTS="$OPTS -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf"` directly to the CLI tool script. The meaning of configurations is the same as the meaning of configurations in Java client section.

## Configure Kerberos authentication between ZooKeeper and broker

Pulsar broker acts as a Kerberos client when authenticating with Zookeeper.

1. Add the settings in `conf/zookeeper.conf`.

   ```conf
   authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
   requireClientAuthScheme=sasl
   ```

2. Enter the following commands to add a section of `Client` configurations in `pulsar_jaas.conf` that Pulsar broker uses:

   ```
    Client {
      com.sun.security.auth.module.Krb5LoginModule required
      useKeyTab=true
      storeKey=true
      useTicketCache=false
      keyTab="/etc/security/keytabs/pulsarbroker.keytab"
      principal="broker/localhost@EXAMPLE.COM";
   };
   ```

   In this setting, the principal of Pulsar broker and keytab file indicates the role of brokers when you authenticate with ZooKeeper.

For more information, see [ZooKeeper document](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Client-Server+mutual+authentication)

## Configure Kerberos authentication for BookKeeper and broker

Pulsar broker acts as a Kerberos client when authenticating with Bookie. 

1. Add the `bookkeeperClientAuthenticationPlugin` parameter in `broker.conf`.

   ```conf
   bookkeeperClientAuthenticationPlugin=org.apache.bookkeeper.sasl.SASLClientProviderFactory
   ```

   `SASLClientProviderFactory` creates a BookKeeper SASL client in a broker, and the broker uses the created SASL client to authenticate with a Bookie node.

2. Add a section of `BookKeeper` configurations in the `pulsar_jaas.conf` file that broker/proxy uses.

   ```conf
    BookKeeper {
      com.sun.security.auth.module.Krb5LoginModule required
      useKeyTab=true
      storeKey=true
      useTicketCache=false
      keyTab="/etc/security/keytabs/pulsarbroker.keytab"
      principal="broker/localhost@EXAMPLE.COM";
   };
   ```

   In this setting, the principal of Pulsar broker and keytab file indicates the role of brokers when you authenticate with Bookie.

For more information, see [BookKeeper document](https://bookkeeper.apache.org/docs/next/security/sasl/).