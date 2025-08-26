# PIP-30: change authentication provider API to support mutual authentication

* **Status**: Implemented
* **Author**: Jia Zhai
* **Pull Request**: https://github.com/apache/pulsar/pull/3677
* **Mailing List discussion**:
* **Release**: 2.4.0


## Motivation

Pulsar has a [pluggable authentication mechanism](http://pulsar.apache.org/docs/en/security-extending/#authentication) that currently supports several auth providers.
But currently all the provided authentication are a kind of “single-step" authentication. And under current api it is not able to support mutual authentication between client and server， such as [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer). 
So this PIP is try to discuss the interface changes to support mutual authentication.

## Proposal

In Pulsar, authentication is happened when a new connection is creating between client and broker. When connecting, Client sends authentication data to Broker by `CommandConnect`, and Broker do the authentication and once success send command `CommandConnected` back to client.

In PulsarApi.proto, [CommandConnect](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto#L173), it contains `auth_method_name` and `auth_data` fields.  But broker no need to send auth data to client, so add new command CommandAuthResponse and CommandAuthChallenge to carry the data between client and broker.

```
message CommandAuthResponse {
	optional string client_version = 1;
	optional AuthData response = 2;
	optional int32 protocol_version = 3 [default = 0];
}

message CommandAuthChallenge {
	optional string server_version = 1;
	optional AuthData challenge = 2;
	optional int32 protocol_version = 3 [default = 0];
}

// To support mutual authentication type, such as Sasl, reuse this command to mutual auth.
message AuthData {
	optional string auth_method_name = 1;
	optional bytes auth_data = 2;
}
```

The propose is to reuse these 2 commands related to connecting and auth, and also add auth data fields in CommandConnected. So when server need send auth data back to client, broker could reuse command CommandConnected. 

A basic logic for the mutual authentication is like this :

1, Client side newConnectCommand(init auth) and send to Broker;
2, Broker side handleConnect(authDataClient),  do the auth in Broker side, and get authDataBroker.
- If auth is complete Broker.newConnected(), finish the auth, and send command back to Client.
- If auth is not complete, Broker.newAuthChallenge(authDataBroker) and send command back to Client.
3, Client side
- If received Connected command, complete the auth, and connection established. 
- If received AuthChallenge command, do the auth with authDataBroker, and get authDataClient, then send AuthResponse back to Broker. Broker will repeat the process of step 2 until auth complete.


## Changes

### Proto


### API changes

#### setCommandData changes in `AuthenticationDataSource` and `AuthenticationDataProvider`:

When establish connection(between client and broker), in each connection, Broker side will have a [AuthenticationDataSource](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationDataSource.java), and Client side will have a 
[AuthenticationDataProvider.java](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationDataProvider.java). They both have a `getCommandData` method, which get command data from proto command.
To achieve the mutual authn, we need a method `authenticate`, which instead `getCommandData` but will compute use passed in data and return evaluated and challenged data back to peer.

And since in PulsarApi.proto, we store auth data as bytes in [CommandConnected](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto#L197) and `CommandConnected` command, and in [SaslServer](https://docs.oracle.com/javase/7/docs/api/javax/security/sasl/SaslServer.html) and [SaslClient](https://docs.oracle.com/javase/7/docs/api/javax/security/sasl/SaslClient.html), it do the valuate and challenge also use bytes, like this:
```
byte[]  evaluateResponse(byte[] response)
```
It would be better to use byte[] in method `authenticate`, this could avoid the converting 
`bytes(proto cmd) <-> String(pulsar api) <-> bytes(sasl api)`
each time. 


Client change in: [AuthenticationDataProvider](https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/AuthenticationDataProvider.java) 

```
    /**
     * @return authentication data which is stored in a command
     */
    default String getCommandData() {
        return null;
    }

    /**
     * For mutual authentication, This method use passed in `data` to evaluate and challenge,
     * then returns null if authentication has completed;
     * returns authenticated data back to peer side, if authentication has not completed.
     *
     * used for mutual authentication like sasl.
     */
    default byte[] authenticate(byte[] data) throws IOException { // << add this method
        throw new UnsupportedOperationException();
    }
```

Additionally, broker side will check whether `authenticate` returns null or not to know whether the authentication between client and broker is completed. And Broker could make decision of send different command back to Client. If it is complete, send Connected command, and client will stop the mutual authn; else send Connecting command, and client will continue mutual authn.
Broker change in: [AuthenticationDataSource](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationDataSource.java)

```
    /**
     * @return authentication data which is stored in a command
     */
    default String getCommandData() {
        return null;
    }

    /**
     * For mutual authentication, This method use passed in `data` to evaluate and challenge,
     * then returns null if authentication has completed;
     * returns authenticated data back to peer side, if authentication has not completed.
     *
     * used for mutual authentication like sasl.
     */
    default byte[] authenticate(byte[] data) throws IOException { // << add this method
        throw new UnsupportedOperationException();
    }
```



#### Return the specific `AuthenticationDataProvider` for each new Connection in ClientCnx

As mentioned above, we leverage [AuthenticationDataProvider](https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/AuthenticationDataProvider.java) in client to keep authn data between [ClientCnx](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ClientCnx.java) and Broker. But current [Authentication](https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/Authentication.java) not have a way to get a specific `AuthenticationDataProvider` to connect with the broker that the client will talk to.
Need to add a method to Get/Create an `AuthenticationDataProvider` which provides the data provider that stands for the specific broker.

```
    /**
     *
     * Get/Create an authentication data provider which provides the data that this client will be sent to the broker.
     * Some authentication method need to authn between each client channel. So it need the broker, who it will talk to.
     *
     * @param brokerHostName
     *          target broker host name
     *
     * @return The authentication data provider
     */
    default AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        throw new UnsupportedAuthenticationException("Method not implemented!");
    }
```


#### Bring in an `AuthState` interface to unify the auth process in ServerCnx.

This is a suggestion from @sijie, in this [codereview](https://github.com/apache/pulsar/pull/3658#discussion_r259224620).
An `AuthState` basically is holding the authentication state, tell broker whether the authentication is completed or not, if completed, what is the AuthRole.

```
interface AuthState {
    String getAuthRole();

    /**
      * Returns null if authentication has completed, and no auth data is required to send back to client.
      * Returns the auth data back to client, if authentication has not completed.
      */
    byte[] authenticate(byte[] authData);

    boolean isComplete();
}
```

Then add a `newAuthState` in the [AuthenticationProvider](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProvider.java). The default implementation can be the `OneStageAuthState`, It can be shared across existing authentication providers. 


So the implementation in serverCnx can be very simple:

```
AuthState authState = authenticationProvider.newAuthState();

byte[] clientAuthData = connect.getAuthData().toByteArray();
byte[] brokerAuthData = authState.authenticate(clientAuthData);
if (authState.isComplete()) {
     // authentication has completed.
     authRole = authState.getAuthRole();
     // we are done here
} else {
     // it is a multi-stage authentication mechanism, send the auth data back
     ctx.writeAndFlush(Commands.newConnecting(authMethod, data));
}
```

For the current existing authentication providers, implement a common OneStageAuthState.
For the mutual authenciation provider, like SASL, implement a SaslAuthState.
This would produce a clean interface between AuthenticationProvider and Brokers and avoiding add Sasl specific logic in ServerCnx.


Besides this in [AuthenticationProvider](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProvider.java), it need a method `getAuthDataSource()`, which get a specific [AuthenticationDataSource](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationDataSource.java) stands for a connection in broker:

```
    /**
     * Get/Create an authentication data provider which provides the data that this broker will be sent to the client.
     * Some authentication method need to auth between each client channel.
     */
    default AuthenticationDataSource getAuthDataSource() throws IOException {
        throw new UnsupportedOperationException();
    }

    default AuthenticationState newAuthState(AuthenticationDataSource authenticationDataSource) {
        return new OneStageAuthenticationState(authenticationDataSource, this);
    }
```

To reference a more detailed changes at [here](https://github.com/apache/pulsar/pull/3677)
