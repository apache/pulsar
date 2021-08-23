---
id: version-2.4.0-security-kerberos
title: Authentication using Kerberos
sidebar_label: Authentication using Kerberos
original_id: security-kerberos
---

[Kerberos](https://web.mit.edu/kerberos/) is a network authentication protocol. It is designed to provide strong authentication for client/server applications by using secret-key cryptography. 

In Pulsar, we use Kerberos with [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer) as a choice for authentication. And Pulsar uses the [Java Authentication and Authorization Service (JAAS)](https://en.wikipedia.org/wiki/Java_Authentication_and_Authorization_Service) for SASL configuration. You must provide JAAS configurations for Kerberos authentication. 

In this document, we will introduce how to configure `Kerberos` with `SASL` between Pulsar clients and brokers in detail, and then how to configure Kerberos for Pulsar proxy.

## Configuration for Kerberos between Client and Broker

### Prerequisites

To begin, you need to set up(or already have) a [Key Distribution Center(KDC)](https://en.wikipedia.org/wiki/Key_distribution_center) configured and running. 

If your organization is already using a Kerberos server (for example, by using `Active Directory`), there is no need to install a new server for Pulsar. Otherwise you will need to install one. Your Linux vendor likely has packages for `Kerberos` and a short guide on how to install and configure it: ([Ubuntu](https://help.ubuntu.com/community/Kerberos), 
[Redhat](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html)).

Note that if you are using Oracle Java, you need to download JCE policy files for your Java version and copy them to the `$JAVA_HOME/jre/lib/security` directory.

#### Kerberos Principals

If you are using existing Kerberos system, ask your Kerberos administrator for a principal for each Brokers in your cluster and for every operating system user that will access Pulsar with Kerberos authentication(via clients and tools).

If you have installed your own Kerberos system, you can create these principals with the following commands:

```shell
### add Principals for broker
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}"
### add Principals for client
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}"
```

Note that it is a *Kerberos* requirement that all your hosts can be resolved with their FQDNs.

The first part of Broker principal (for example, `broker` in `broker/{hostname}@{REALM}`) is the `serverType` of each host, 
The suggested values of `serverType` are `broker` (host machine runs service Pulsar Broker) and `proxy` (host machine runs service Pulsar Proxy). 

#### Configure how to connect to KDC

You need to specify the path to the `krb5.conf` file for both client and broker side. The contents of `krb5.conf` file indicate the default Realm and KDC information. See [JDK’s Kerberos Requirements](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details.

```shell
-Djava.security.krb5.conf=/etc/pulsar/krb5.conf
```
Here is an example of the krb5.conf file:
 
In the configuration file, `EXAMPLE.COM` is the default realm; `kdc = localhost:62037` is the kdc server url for realm `EXAMPLE.COM `:

```
[libdefaults]
 default_realm = EXAMPLE.COM

[realms]
 EXAMPLE.COM  = {
  kdc = localhost:62037
 }
```

Usually machines configured with kerberos already have a system wide configuration and this configuration is optional.

#### JAAS configuration file

JAAS configuration file is needed for both client and broker sides. It provides the section of information that used to connect KDC. Here is an example named `pulsar_jaas.conf`:

```
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

You need to set the `JAAS` configuration file path as JVM parameter for client and broker. For example:

```shell
    -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf 
```

In the `pulsar_jaas.conf` file above 

1. `PulsarBroker` is a section name in the JAAS file used by each broker. This section tells the broker which principal to use inside Kerberos
    and the location of the keytab where the principal is stored. It allows the broker to use the keytab specified in this section.
2. `PulsarClient` is a section name in the JASS file used by each client. This section tells the client which principal to use inside Kerberos
    and the location of the keytab where the principal is stored. It allows the client to use the keytab specified in this section.
    In the following example, this `PulsarClient` section will also be reused in both the Pulsar internal admin configuration and in CLI command of `bin/pulsar-client`, `bin/pulsar-perf` and `bin/pulsar-admin`. You can also add different sections for different use cases.

You can have 2 separate JAAS configuration files: 
* the file for a broker has sections of both `PulsarBroker` and `PulsarClient`; 
* the file for a client only has a `PulsarClient` section.


### Kerberos configuration for Brokers

#### Configure `broker.conf` file
 
 In the `broker.conf` file, set Kerberos related configurations.

 - Set `authenticationEnabled` to `true`;
 - Set `authenticationProviders` to choose `AuthenticationProviderSasl`;
 - Set `saslJaasClientAllowedIds` regex for principal that is allowed to connect to broker;
 - Set `saslJaasBrokerSectionName` that corresponding to the section in JAAS configuration file for broker;
 
 To make Pulsar internal admin client work properly, you need to set the configuration in the `broker.conf` file as below: 
 - Set `brokerClientAuthenticationPlugin` to client plugin `AuthenticationSasl`;
 - Set `brokerClientAuthenticationParameters` to value in JSON string `{"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}`, in which `PulsarClient` is the section name in above `pulsar_jaas.conf` file, and `"serverType":"broker"` indicate that internal admin client will connect to a Pulsar Broker;

 Here is an example:

```
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarBroker

## Authentication settings of the broker itself. Used when the broker connects to other brokers
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
brokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}
```

#### Set Broker JVM parameter

 Set JVM parameters for JAAS configuration file and krb5 configuration file with additional options.
```shell
   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf 
```
You can add this at the end of `PULSAR_EXTRA_OPTS` in the file [`pulsar_env.sh`](https://github.com/apache/pulsar/blob/master/conf/pulsar_env.sh)

Make sure that the keytabs configured in the `pulsar_jaas.conf` file and kdc server in the `krb5.conf` file are reachable by the operating system user who is starting broker.


### Kerberos configuration for clients

#### Java Client and Java Admin Client

In client application, include `pulsar-client-auth-sasl` in your project dependency.

```
    <dependency>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client-auth-sasl</artifactId>
      <version>${pulsar.version}</version>
    </dependency>
```

configure the authentication type to use `AuthenticationSasl`, and also provide the authentication parameters to it. 

There are 2 parameters needed: 
- `saslJaasClientSectionName` is corresponding to the section in JAAS configuration file for client; 
- `serverType` stands for whether this client is connect to broker or proxy, and client use this parameter to know which server side principal should be used. 

When authenticate between client and broker with the setting in above JAAS configuration file, we need to set `saslJaasClientSectionName` to `PulsarClient` and `serverType` to `broker`.

The following is an example of creating a Java client:
 
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

> The first two lines in the example above are hard coded, alternatively, you can set additional JVM parameters for JAAS and krb5 configuration file when running the application like below:

```
java -cp -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf $APP-jar-with-dependencies.jar $CLASSNAME
```

Make sure that the keytabs configured in the `pulsar_jaas.conf` file and kdc server in the `krb5.conf` file are reachable by the operating system user who is starting pulsar client.

#### Configure CLI tools

If you are using a command-line tool (such as `bin/pulsar-client`, `bin/pulsar-perf` and `bin/pulsar-admin`), you need to perform the following steps:

Step 1. Config your `client.conf`.
```shell
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
authParams={"saslJaasClientSectionName":"PulsarClient", "serverType":"broker"}
```


Step 2. Set JVM parameters for JAAS configuration file and krb5 configuration file with additional options.
```shell
   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf 
```

You can add this at the end of `PULSAR_EXTRA_OPTS` in the file [`pulsar_tools_env.sh`](https://github.com/apache/pulsar/blob/master/conf/pulsar_tools_env.sh),
or add this line `OPTS="$OPTS -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf "` directly to the CLI tool script.

The meaning of configurations is the same as that in Java client section.

##  Kerberos configuration for working with Pulsar Proxy

With the above configuration, client and broker can do authentication using Kerberos.  

If a client wants to connect to Pulsar Proxy, it is a little different. Client (as a SASL client in Kerberos) will be authenticated by Pulsar Proxy (as a SASL Server in Kerberos) first; and then Pulsar Proxy will be authenticated by Pulsar broker. 

Now comparing with the above configuration between client and broker, we will show how to configure Pulsar Proxy. 

### Create principal for Pulsar Proxy in Kerberos

Comparing with the above configuration, you need to add new principal for Pulsar Proxy. If you already have principals for client and broker, only add proxy principal here.

```shell
### add Principals for Pulsar Proxy
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey proxy/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{proxy-keytabname}.keytab proxy/{hostname}@{REALM}"
### add Principals for broker
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}"
### add Principals for client
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}"
```

### Add a section in JAAS configuration file for Pulsar Proxy

Comparing with the above configuration, add a new section for Pulsar Proxy in JAAS configuration file.

Here is an example named `pulsar_jaas.conf`:

```
 PulsarBroker {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarbroker.keytab"
   principal="broker/localhost@EXAMPLE.COM";
};

 PulsarProxy {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarproxy.keytab"
   principal="proxy/localhost@EXAMPLE.COM";
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

### Proxy Client configuration

Pulsar client configuration is similar with client and broker configuration, except that `serverType` is set to `proxy` instead of `broker`, because it needs to do Kerberos authentication between client and proxy.

 ```java
 System.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");
 System.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");

 Map<String, String> authParams = Maps.newHashMap();
 authParams.put("saslJaasClientSectionName", "PulsarClient");
 authParams.put("serverType", "proxy");        // ** here is the different **

 Authentication saslAuth = AuthenticationFactory
         .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);
 
 PulsarClient client = PulsarClient.builder()
         .serviceUrl("pulsar://my-broker.com:6650")
         .authentication(saslAuth)
         .build();
 ```

> The first two lines in the example above are hard coded, alternatively, you can set additional JVM parameters for JAAS and krb5 configuration file when running the application like below:

```
java -cp -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf $APP-jar-with-dependencies.jar $CLASSNAME
```

### Kerberos configuration for Pulsar Proxy service

In the `proxy.conf` file, set Kerberos related configuration. Here is an example:
```shell
## related to authenticate client.
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarProxy

## related to be authenticated by broker
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
brokerClientAuthenticationParameters={"saslJaasClientSectionName":"PulsarProxy", "serverType":"broker"}
forwardAuthorizationCredentials=true
```

The first part is related to authenticate between client and Pulsar Proxy. In this phase, client works as SASL client, while Pulsar Proxy works as SASL server. 

The second part is related to authenticate between Pulsar Proxy and Pulsar Broker. In this phase, Pulsar Proxy works as SASL client, while Pulsar Broker works as SASL server.

### Broker side configuration.

The broker side configuration file is the same with the above `broker.conf`, you do not need special configuration for Pulsar Proxy.

```
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarBroker
```

## Regarding authorization and role token

For Kerberos authentication, the authenticated principal is used as the role token for Pulsar authorization.  For more information of authorization in Pulsar, see [security authorization](security-authorization.md).

If you enabled authorizationEnabled you need set `superUserRoles` in `broker.conf` that corresponding to the name registered in kdc

For example:
```bash
superUserRoles=client/{clientIp}@EXAMPLE.COM
```

## Regarding authentication between ZooKeeper and Broker

Pulsar Broker acts as a Kerberos client when authenticating with Zookeeper. According to [ZooKeeper document](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Client-Server+mutual+authentication), you need these settings in `conf/zookeeper.conf`:

```
authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
requireClientAuthScheme=sasl
```

And add a section of `Client` configurations in the file `pulsar_jaas.conf`, which is used by Pulsar Broker:

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

In this setting, Pulsar Broker's principal and keyTab file indicates Broker's role when authenticating with ZooKeeper.

## Regarding authentication between BookKeeper and Broker

Pulsar Broker acts as a Kerberos client when authenticating with Bookie. According to [BookKeeper document](http://bookkeeper.apache.org/docs/latest/security/sasl/), you need to add `bookkeeperClientAuthenticationPlugin` parameter in `broker.conf`:

```
bookkeeperClientAuthenticationPlugin=org.apache.bookkeeper.sasl.SASLClientProviderFactory
```

In this setting, `SASLClientProviderFactory` creates a BookKeeper SASL client in a Broker, and the Broker uses the created SASL client to authenticate with a Bookie node.

And add a section of `BookKeeper` configurations in the `pulsar_jaas.conf` that used by Pulsar Broker:

```
 BookKeeper {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   storeKey=true
   useTicketCache=false
   keyTab="/etc/security/keytabs/pulsarbroker.keytab"
   principal="broker/localhost@EXAMPLE.COM";
};
```

In this setting, Pulsar Broker's principal and keyTab file indicates Broker's role when authenticating with Bookie.
