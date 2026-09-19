# PIP-85: Expose Pulsar-Client via Function/Connector BaseContext

- Status: Proposal
- Authors: Neng Lu
- Pull Request: https://github.com/apache/pulsar/pull/11056
- Mailing List discussion:
- Release:

## Motivation

Providing Functions/Connectors the ability to securely and easily access the underlying Pulsar Cluster is very useful in many scenarios. It enables functions/connectors to utilize Pulsar Cluster as a backend store in addition to normal data processing path.

Currently, in order to access the pulsar cluster, users have to create the pulsar client on their own and provide all the auth parameters. This way is complicated, insecure, and unnecessary.

Instead, we should have a standard way to allow users to access to the pulsar client easily and securely.

## API and Implementation Changes

As we have refactored the organization of the function's context object hierarchy, it currently looks like the right side of the following graph:

[![context hierarchy](https://user-images.githubusercontent.com/16407807/118730483-8ebf5200-b7ec-11eb-9220-d41261f148bb.png)](https://user-images.githubusercontent.com/16407807/118730483-8ebf5200-b7ec-11eb-9220-d41261f148bb.png)

To allow Function, Source Connector, and Sink Connector all have the ability to access the pulsar cluster, we need to introduce a new API in the `BaseContext` object:

```java
    /**
     * Get the pulsar client.
     *
     * @return the instance of pulsar client
     */
    default PulsarClient getPulsarClient() {
        throw new UnsupportedOperationException("not implemented");
    }
```

And for the implementation, the `ContextImpl` object should return the `PulsarClient` it's using for various purposes:

```java
    @Override
    public PulsarClient getPulsarClient() {
        return client;
    }
```

Notice, this return `PulsarClient` connects to the underlying pulsar cluster already and has auth parameters inherited from function/source/sink submitter. So it should be properly scoped and will only be able to perform operations that it's authorized to do.

To utilize the client in function/source/sink code is fairly straightforward.

```java

    public String func(String input, Context context) {
      	PulsarClient client = context.getPulsarClient();
				....
      	return ...;
    }
```
