---
id: admin-api-get-started
title: Get started
sidebar_label: "Get started"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

## Admin setup

Each of the three admin interfaces (the `pulsar-admin` CLI tool, the {@inject: rest:REST:/} API, and the [Java admin API](/api/admin)) requires some special setup if you have enabled authentication in your Pulsar instance.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

If you have enabled authentication, you need to provide an auth configuration to use the `pulsar-admin` tool. By default, the configuration for the `pulsar-admin` tool is in the [`conf/client.conf`](reference-configuration.md#client) file. The following are the available parameters:

|Name|Description|Default|
|----|-----------|-------|
|webServiceUrl|The web URL for the cluster.|http://localhost:8080/|
|brokerServiceUrl|The Pulsar protocol URL for the cluster.|pulsar://localhost:6650/|
|authPlugin|The authentication plugin.| |
|authParams|The authentication parameters for the cluster, as a comma-separated string.| |
|useTls|Whether or not TLS authentication will be enforced in the cluster.|false|
|tlsAllowInsecureConnection|Accept untrusted TLS certificate from client.|false|
|tlsTrustCertsFilePath|Path for the trusted TLS certificate file.| |

</TabItem>
<TabItem value="REST API">

You can find details for the REST API exposed by Pulsar brokers in this {@inject: rest:document:/}.

</TabItem>
<TabItem value="Java">

To use the Java admin API, instantiate a {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object, and specify a URL for a Pulsar broker and a {@inject: javadoc:PulsarAdminBuilder:/admin/org/apache/pulsar/client/admin/PulsarAdminBuilder}. The following is a minimal example using `localhost`.

```java
String url = "http://localhost:8080";
// Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authPluginClassName = "com.org.MyAuthPluginClass";
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;
PulsarAdmin admin = PulsarAdmin.builder()
    .authentication(authPluginClassName,authParams)
    .serviceHttpUrl(url)
    .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
    .allowTlsInsecureConnection(tlsAllowInsecureConnection)
    .build();
```

If you use multiple brokers, you can use multi-host like Pulsar service. For example,

```java
String url = "http://localhost:8080,localhost:8081,localhost:8082";
// Below are the same to the line 2 - line 13 in the code snippet above
// Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authPluginClassName = "com.org.MyAuthPluginClass";
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;
PulsarAdmin admin = PulsarAdmin.builder()
    .authentication(authPluginClassName,authParams)
    .serviceHttpUrl(url)
    .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
    .allowTlsInsecureConnection(tlsAllowInsecureConnection)
    .build();
```

</TabItem>

</Tabs>
````
