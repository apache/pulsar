# PIP-99: Pulsar Proxy Extensions

**Status**: Accepted

**Author**: Enrico Olivelli

**Pull Request**: https://github.com/apache/pulsar/pull/11838 

**Mailing List discussion**: 
https://lists.apache.org/x/thread.html/r259cfd0d20162e5e24290bc2ea8f8ac74d53489975e0c7b40d326fec@%3Cdev.pulsar.apache.org%3E 

**Release**: 2.9.0

## Motivation

In Pulsar 2.5.0 Pulsar has been enriched by Broker Protocol Handlers (see https://github.com/apache/pulsar/wiki/PIP-41%3A-Pluggable-Protocol-Handler ).
With Broker Protocol Handlers it is possible to add to the Pulsar Broker new implementations of other binary protocols, and such implementation can access the internals of the Pulsar Broker:

- in a very efficient way (zero-copy)
- leveraging Broker discovery/Topic features
- leveraging Pulsar Authentication and Authorization facilities
- deploying the Protocol Handler code inside the same JVM/Process/Container/Pod of the Pulsar Broker
- leveraging Broker configuration facilities

When you deploy Pulsar with the Pulsar Proxy, especially when you run in Kubernetes, you  cannot use the Broker Protocol Handlers of PIP-41, this is because the Proxy handles only the Pulsar binary protocol and the Pulsar HTTP API.

This PIP aims to add a mechanism similar to the one we have in the Pulsar Broker, but in the Pulsar Proxy.

Adding **Proxy Extensions** (PE) to the Pulsar proxy, you will see these benefits:

- the PE can use the ProxyConfiguration facilities, enabling seamless integration and configuration (like bin/pulsar proxy and conf/proxy.conf)
- The PE can use Authentication and Authorization mechanisms built-in the Pulsar
- The PE can use the same services for Broker discovery and Topic Lookup
- In a Kubernetes environment (or any other proxy based deployment) you have to expose only the Proxy Service, no need to add additional services

We are going to follow the same conventions of the Broker Protocol Handlers and of the Pulsar Proxy, in order to make the installation of Proxy Extensions very intuitive and straightforward for the users:
- Configuration in proxy.conf
- Using NAR packaging
- Use a dedicated directory pulsar/proxyextensions 

## Public Interfaces
This PIP will add the same set of Interterfaces we have for the Broker Protocol Handlers, with these differences:
The package name won’t be org.apache.pulsar.broker.protocol but org.apache.pulsar.proxy.protocol
The Proxy Extension will have access to the ProxyService  instance and not to the BrokerService instance

On the proxy configuration we are going to add two new entries “proxyExtensionsDirectory” and “proxyExtensions”:
```

    /***** --- Extensions --- ****/
    @FieldContext(
      category = CATEGORY_PLUGIN,
      doc = "The directory to locate extensions"
    )
    private String proxyExtensionsDirectory = "./proxyextensions";

    @FieldContext(
      category = CATEGORY_PLUGIN,
      doc = "List of extensions to load…"
    )
    private Set<String> proxyExtensions = Sets.newTreeSet();
```

## Proposed Changes

We will introduce a new set of classes that implement the Proxy Extensions, the code will look like the code of the Pulsar Broker.
See [PIP-41 A-Pluggable-Protocol-Handler ](https://github.com/apache/pulsar/wiki/PIP-41%3A-Pluggable-Protocol-Handler )

The Proxy Extensions will not have support for Advertising Custom Protocol Metadata (paragraph “ADVERTISE”, function `getProtocolDataToAdvertise`), as there is currently no support in Pulsar for advertising the presence of Pulsar Proxy and this is not likely to be needed in the short term.
The code base of the Pulsar Proxy and the Pulsar Broker are in different Maven Modules and the amount of code is very small, so the classes will be basically copied and adjusted:
- new package name
- references to ProxyService
- new ProxyConfiguration entries
- removal of getProtocolDataToAdvertise

The yaml file with the definition of the Proxy Extensions will be ‘pulsar-proxy-extension.yml’ and not ‘pulsar-protocol-handler.yml’
New tests will be added about the startup and shutdown of the proxy, mimicking the same tests on the Broker side.

## Compatibility, Deprecation, and Migration Plan
This is a new feature, without impact on existing users.

## Test Plan
We will add new unit tests that cover the new code.

## Rejected Alternatives
Let users implement their own proxy and deploy next to the proxy process/container/pod.
This is rejected because it will make it harder to deploy Pulsar in Kubernetes and also it will make developers code more and more mechanisms for Broker Discovery, Topic Lookup, Authentication and Authorization.
