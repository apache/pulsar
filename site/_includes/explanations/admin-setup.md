Each of Pulsar's three admin interfaces---the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool, the [Java admin API](/api/admin), and the [REST API](../../reference/RestApi)---requires some special setup if you have [authentication](../../admin/Authz#authentication-providers) enabled in your Pulsar {% popover instance %}.

### pulsar-admin

If you have [authentication](../../admin/Authz#authentication-providers) enabled, you will need to provide an auth configuration to use the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool. By default, the configuration for the `pulsar-admin` tool is found in the [`conf/client.conf`](../../reference/Configuration#client) file. Here are the available parameters:

{% include config.html id="client" %}

### REST API

You can find documentation for the REST API exposed by Pulsar {% popover brokers %} in [this reference document](../../reference/RestApi).

### Java admin client

To use the Java admin API, instantiate a {% javadoc PulsarAdmin admin com.yahoo.pulsar.client.admin.PulsarAdmin %} object, specifying a URL for a Pulsar {% popover broker %} and a {% javadoc ClientConfiguration admin com.yahoo.pulsar.client.admin.ClientConfiguration %}. Here's a minimal example using `localhost`:

```java
URL url = new URL("http://localhost:8080");
String authPluginClassName = "com.org.MyAuthPluginClass"; //Pass auth-plugin class fully-qualified name if Pulsar-security enabled
String authParams = "param1=value1";//Pass auth-param if auth-plugin class requires it
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```
