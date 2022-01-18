---
id: version-2.7.4-admin-api-overview
title: The Pulsar admin interface
sidebar_label: Overview
original_id: admin-api-overview
---

The Pulsar admin interface enables you to manage all of the important entities in a Pulsar [instance](reference-terminology.md#instance), such as [tenants](reference-terminology.md#tenant), [topics](reference-terminology.md#topic), and [namespaces](reference-terminology.md#namespace).

You can currently interact with the admin interface via:

- Making HTTP calls against the admin {@inject: rest:REST:/} API provided by Pulsar [brokers](reference-terminology.md#broker). For some restful apis, they might be redirected to topic owner brokers for serving
   with [`307 Temporary Redirect`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/307), hence the HTTP callers should handle `307 Temporary Redirect`. If you are using `curl`, you should specify `-L`
   to handle redirections.
- The `pulsar-admin` CLI tool, which is available in the `bin` folder of your [Pulsar installation](getting-started-standalone.md):

  ```shell
  $ bin/pulsar-admin
  ```

  For the complete commands and descriptions of `pulsar-admin`, see [here](https://pulsar.apache.org/tools/pulsar-admin/2.7.0-SNAPSHOT/).

- A Java client interface.

> #### The REST API is the admin interface
> Under the hood, both the `pulsar-admin` CLI tool and the Java client both use the REST API. If you’d like to implement your own admin interface client, you should use the REST API as well. Full documentation can be found here.

In this document, examples from each of the three available interfaces will be shown.

## Admin setup

Each of Pulsar's three admin interfaces---the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool, the [Java admin API](/api/admin), and the {@inject: rest:REST:/} API ---requires some special setup if you have [authentication](security-overview.md#authentication-providers) enabled in your Pulsar [instance](reference-terminology.md#instance).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

If you have [authentication](security-overview.md#authentication-providers) enabled, you will need to provide an auth configuration to use the [`pulsar-admin`](reference-pulsar-admin.md) tool. By default, the configuration for the `pulsar-admin` tool is found in the [`conf/client.conf`](reference-configuration.md#client) file. Here are the available parameters:

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

You can find documentation for the REST API exposed by Pulsar [brokers](reference-terminology.md#broker) in this reference {@inject: rest:document:/}.

<!--Java-->

To use the Java admin API, instantiate a {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object, specifying a URL for a Pulsar [broker](reference-terminology.md#broker) and a {@inject: javadoc:PulsarAdminBuilder:/admin/org/apache/pulsar/client/admin/PulsarAdminBuilder}. Here's a minimal example using `localhost`:

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

If you have multiple brokers to use, you can use multi-host like Pulsar service. For example,
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

If you run Pulsar Functions or connectors on Kubernetes, you need to follow Kubernetes naming convention to define the names of your Pulsar resources, whichever admin interface you use.

Kubernetes requires a name that can be used as a DNS subdomain name as defined in [RFC 1123](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names). Pulsar supports more legal characters than Kubernetes naming convention. If you create a Pulsar resource name with special characters that are not supported by Kubernetes (for example, including colons in a Pulsar namespace name), Kubernetes runtime translates the Pulsar object names into Kubernetes resource labels which are in RFC 1123-compliant forms. Consequently, you can run functions or connectors using Kubernetes runtime. The rules for translating Pulsar object names into Kubernetes resource labels are as below:

- Truncate to 63 characters
  
- Replace the following characters with dashes (-):
  
  - Non-alphanumeric characters
  
  - Underscores (_)
  
  - Dots (.) 
  
- Replace beginning and ending non-alphanumeric characters with 0
  
> **Tip**
> 
> - If you get an error in translating Pulsar object names into Kubernetes resource labels (for example, you may have a naming collision if your Pulsar object name is too long) or want to customize the translating rules, see [customize Kubernetes runtime](https://pulsar.apache.org/docs/en/next/functions-runtime/#customize-kubernetes-runtime).
> 
> - For how to configure Kubernetes runtime, see [here](https://pulsar.apache.org/docs/en/next/functions-runtime/#configure-kubernetes-runtime).
