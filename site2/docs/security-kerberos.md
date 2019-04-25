---
id: security-kerberos
title: Authentication using Kerberos
sidebar_label: Authentication using Kerberos
---

[Kerberos](https://web.mit.edu/kerberos/) is a network authentication protocol. It is designed to provide strong authentication for client/server applications by using secret-key cryptography. 

In Pulsar, we use Kerberos with [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer) as a choice for authentication. While in SASL, Pulsar uses the [Java Authentication and Authorization Service (JAAS)](https://en.wikipedia.org/wiki/Java_Authentication_and_Authorization_Service) for SASL configuration. You must provide JAAS configurations for Kerberos authentication. 

In this documentation, We will start with a general description of how to configure `Kerberos` with `SASL` between Pulsar clients and brokers, followed by some operational details. Then comparing with it, we will show how to configure for Pulsar Proxy.

## Configuration for Kerberos between Client and Broker

### Prerequisites

To begin, you need to set up(or already have) a [Key Distribution Center](KDC) configured and running. 

If your organization is already using a Kerberos server (for example, by using `Active Directory`), there is no need to install a new server just for Pulsar. Otherwise you will need to install one, your Linux vendor likely has packages for `Kerberos` and a short guide on how to install and configure it ([Ubuntu](https://help.ubuntu.com/community/Kerberos),
[Redhat](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html)).

Note that if you are using Oracle Java, you will need to download JCE policy files for your Java version and copy them to `$JAVA_HOME/jre/lib/security`.

#### Kerberos Principals

If you are using the organization’s Kerberos or Active Directory server, ask your Kerberos administrator for a principal for each Brokers in your cluster and for every operating system user that will access Pulsar with Kerberos authentication(via clients and tools).

If you have installed your own Kerberos, you will need to create these principals yourself using the following commands:

```shell
### add Principals for broker
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey broker/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{broker-keytabname}.keytab broker/{hostname}@{REALM}"
### add Principals for client
sudo /usr/sbin/kadmin.local -q 'addprinc -randkey client/{hostname}@{REALM}'
sudo /usr/sbin/kadmin.local -q "ktadd -k /etc/security/keytabs/{client-keytabname}.keytab client/{hostname}@{REALM}"
```
Note that it is a *Kerberos* requirement that all your hosts can be resolved with their FQDNs.

#### Configure how to connect to KDC

User need to specify the path to the `krb5.conf` file for both client and broker side. `krb5.conf` indicate the default Realm and KDC information. see [JDK’s Kerberos Requirements](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/KerberosReq.html) for more details):

```shell
-Djava.security.krb5.conf=/etc/pulsar/krb5.conf
```
Here is an example of krb5.conf file:
 
In this configuration file, it provides `EXAMPLE.COM` as default realm; set `udp_preference_limit = 1` to force use TCP; set kdc server url `kdc = localhost:62037` for realm `EXAMPLE.COM `:

```
[libdefaults]
 default_realm = EXAMPLE.COM
 udp_preference_limit = 1

[realms]
 EXAMPLE.COM  = {
  kdc = localhost:62037
 }
```

#### JAAS configuration file

JAAS configuration file is needed for both client and broker side. It provides the section information that will use to connect KDC. Here is an example named `pulsar_jaas.conf`:

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

User need set the `JAAS` configuration file location as JVM parameter to client and broker. For example:

```shell
    -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf 
```

In above `pulsar_jaas.conf` file 

1. `PulsarBroker` is a section name in the JAAS file used by each broker. This section tells the broker which principal to use inside Kerberos
    and the location of the keytab where the principal is stored. It allows the broker to use the keytab specified in this section.
2. `PulsarClient` is a section name in the JASS file used by each client. This section tells the client which principal to use inside Kerberos
    and the location of the keytab where the principal is stored. It allows the client to use the keytab specified in this section.

It is also an option to have 2 separate JAAS configuration files: the file for broker will only have `PulsarBroker` section; while the one for client only have `PulsarClient` section.

### Kerberos configuration for Brokers

1. In `broker.conf` file set Kerberos related configuration.

 - Set `authenticationEnabled` to `true`;
 - Set `authenticationProviders` to choose `AuthenticationProviderSasl`;
 - Set `saslJaasClientAllowedIds` regex for principal that is allowed to connect to broker. 
 - Set `saslJaasBrokerSectionName` that corresponding to the section in JAAS configuration file for broker.
 
 Here is an example:

```
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarBroker
```

2. Set JVM parameter for JAAS configuration file and krb5 configuration file with additional options:
```shell
   -Djava.security.auth.login.config=/etc/pulsar/pulsar_jaas.conf -Djava.security.krb5.conf=/etc/pulsar/krb5.conf 
```
This could be added at the end of `PULSAR_EXTRA_OPTS` in [`pulsar_env.sh`](https://github.com/apache/pulsar/blob/master/conf/pulsar_env.sh)

Make sure the keytabs configured in the `pulsar_jaas.conf` and kdc server in krb5.conf are reachable by the operating system user who is starting broker.

### Kerberos configuration for clients

In client, we need to configure the authentication type to use `AuthenticationSasl`, and also need provide the authentication parameters to it. 

There are 2 parameters needed: 
- `saslJaasClientSectionName` is corresponding to the section in JAAS configuration file for client; 
- `serverType` stands for whether this client is connect to broker or proxy, and client use this parameter to know which server side principal should be used. 

when authenticate between client and broker, and with the setting in above JAAS configuration file, we will set `saslJaasClientSectionName` to `PulsarClient` and `serverType` to `broker`.

Here is an example of creating a Java client 
You can also set an optional `keyId`. Here's an example configuration:
 
 ```java
 System.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");
 System.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");

 Map<String, String> clientSaslConfig = Maps.newHashMap();
 clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
 clientSaslConfig.put("serverType", "broker");

 Authentication saslAuth = AuthenticationFactory
         .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);
 
 PulsarClient client = PulsarClient.builder()
         .serviceUrl("pulsar://my-broker.com:6650")
         .authentication(saslAuth)
         .build();
 ```

Make sure the keytabs configured in the `pulsar_jaas.conf` and kdc server in krb5.conf are reachable by the operating system user who is starting pulsar client.

## Kerberos configuration for working with Pulsar Proxy

With above configuration, client and broker could do authentication using Kerberos.  

If client want to connect to Pulsar Proxy, it is a little different. Client(as a SASL client in Kerberos) will first need be authenticated by Pulsar Proxy(as a SASL Server in Kerberos); then Pulsar Proxy will need be authenticated by Pulsar broker. 

Now comparing with above configuration between client and broker, we will show how to configure Pulsar Proxy. 

### create principal for Pulsar Proxy in Kerberos

Comparing with above configuration, we need add a new principals for Pulsar Proxy. If you already have principals for client and broker, only need to add proxy principal here.

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

Comparing with above configuration, we need add a new section for Pulsar Proxy in JAAS configuration file.

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

Pulsar client configuration is similar with client and broker config, except that `serverType` is set to `proxy` instead of `broker`, because it needs do Kerberos authentication between client and proxy.

 ```java
 System.setProperty("java.security.auth.login.config", "/etc/pulsar/pulsar_jaas.conf");
 System.setProperty("java.security.krb5.conf", "/etc/pulsar/krb5.conf");

 Map<String, String> clientSaslConfig = Maps.newHashMap();
 clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
 clientSaslConfig.put("serverType", "proxy");        // ** here is the different **

 Authentication saslAuth = AuthenticationFactory
         .create(org.apache.pulsar.client.impl.auth.AuthenticationSasl.class.getName(), authParams);
 
 PulsarClient client = PulsarClient.builder()
         .serviceUrl("pulsar://my-broker.com:6650")
         .authentication(saslAuth)
         .build();
 ```

### Kerberos configuration for Pulsar Proxy service

In `proxy.conf` file set Kerberos related configuration. Here is an example:
```shell
## related to authenticate client.
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarProxy

## related to be authenticated by broker
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationSasl
brokerClientAuthenticationParameters=saslJaasClientSectionName:PulsarProxy,serverType:broker
forwardAuthorizationCredentials=true
```

The first part is related to authenticate between client and Pulsar Proxy. In this phase, client works as SASL client, while Pulsar Proxy works as SASL server. 

The second part is related to authenticate between Pulsar Proxy and Pulsar Broker. In this phase, Pulsar Proxy works as SASL client, while Pulsar Broker works as SASL server.

### Broker side configuration.

The broker side configuration file is the same to above `broker.conf`, no need special configuration for Pulsar Proxy.

```
authenticationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl
saslJaasClientAllowedIds=.*client.*
saslJaasBrokerSectionName=PulsarBroker
```

## Regarding authorization and role token

For Kerberos authentication, the authenticated principal is used as the role token for Pulsar authorization.  For more information of authorization in Pulsar, see [this doc](security-authorization.md).


