---
id: admin-api-overview
title: Pulsar admin interface
sidebar_label: Overview
---

The Pulsar admin interface enables you to manage all important entities in a Pulsar instance, such as tenants, topics, and namespaces.

You can interact with the admin interface via:

- Making HTTP calls against the admin {@inject: rest:REST:/} API provided by Pulsar brokers. For some restful APIs, they might be redirected to the topic owner brokers for serving with [`307 Temporary Redirect`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307), hence the HTTP callers should handle `307 Temporary Redirect`. If you use `curl`, you should specify `-L` to handle redirections.
- A Java client interface.
- The `pulsar-admin` CLI tool, which is available in the `bin` folder of your Pulsar installation:

    ```shell
     $ bin/pulsar-admin
    ```

    For details of `pulsar-admin` tool, see the [Pulsar command-line tools](reference-pulsar-admin.md) doc.


> **The REST API is the admin interface**. Both the `pulsar-admin` CLI tool and the Java client use the REST API. If you implement your own admin interface client, you should use the REST API. 

## Admin setup

Each of the three admin interfaces (the `pulsar-admin` CLI tool, the {@inject: rest:REST:/} API, and the [Java admin API](/api/admin)) requires some special setup if you have enabled authentication in your Pulsar instance.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

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

<!--REST API-->

You can find details for the REST API exposed by Pulsar brokers in this {@inject: rest:document:/}.

<!--Java-->

To use the Java admin API, instantiate a {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object, and specify a URL for a Pulsar broker and a {@inject: javadoc:PulsarAdminBuilder:/admin/org/apache/pulsar/client/admin/PulsarAdminBuilder}. The following is a minimal example using `localhost`:

```java
String url = "http://localhost:8080";
// Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authPluginClassName = "com.org.MyAuthPluginClass";
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
boolean useTls = false;
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
// Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authPluginClassName = "com.org.MyAuthPluginClass";
// Pass auth-param if auth-plugin class requires it
String authParams = "param1=value1";
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;
PulsarAdmin admin = PulsarAdmin.builder()
.authentication(authPluginClassName,authParams)
.serviceHttpUrl(url)
.tlsTrustCertsFilePath(tlsTrustCertsFilePath)
.allowTlsInsecureConnection(tlsAllowInsecureConnection)
.build();
```
<!--END_DOCUSAURUS_CODE_TABS-->

## How to define Pulsar resource names when running Pulsar in Kubernetes
If you run Pulsar Functions or connectors on Kubernetes, you need to follow Kubernetes naming convention to define your Pulsar resource name, whichever admin interface you use.

Kubernetes requires a name that can be used as a DNS subdomain name as defined in [RFC 1123](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names). Pulsar supports more legal characters than Kubernetes naming convention. If you create a Pulsar resource name with special characters that is not supported by Kubernetes (for example, including colons in a Pulsar namespace name), you cannot run functions or connectors using Kubernetes runtime.