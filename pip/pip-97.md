# PIP-97: Asynchronous Authentication Provider

Pulsar's current `AuthenticationProvider` interface only exposes synchronous methods for authenticating a connection. To date, this has been sufficient because we do not have any providers that rely on network calls. However, in looking at the OAuth2.0 spec, there are some cases where network calls are necessary to verify a token. As a prerequisite to adding an OAuth2.0 `AuthenticationProvider` (or any other provider that relies on possibly blocking behavior), I propose the addition of asynchronous methods to the `AuthenticationProvider` interface and the `AuthenticationState` interface.

## Goal

Update authentication provider framework to support more authentication protocols (like OAuth2.0 or OpenID Connect) while providing a way to ensure that implementations do not block IO threads.

Target version: ~2.10~ 2.12

## API Changes

In order to prevent confusion, the old `authenticate` methods are deprecated. Three interfaces will need to be updated for this PIP: `AuthenticationProvider`, `AuthenticationState`, and `AuthenticationDataSource`.

The API changes are shown in this PR: https://github.com/apache/pulsar/pull/12104

#### AuthenticationProvider

* Add `AuthenticationProvider#authenticateAsync`. Include a default implementation that calls the `authenticate` method. Note that current implementations should all be non-blocking, so there is no need to push the execution to a separate thread.
* Deprecate `AuthenticationProvider#authenticate`.
* Add `AuthenticationProvider#authenticateHttpRequestAsync`. This method is complicated. It is only called when using the SASL authentication provider (this is hard coded into the Pulsar code base). As such, I would argue that it is worth removing support for this unreachable method and then refactor the SASL authentication provider. I annotated this method with `@InterfaceStability.Unstable` and added details to the Javadoc in order to communicate the uncertainty of this method's future. I am happy to discuss this in more detail though.
* Deprecate `AuthenticationProvider#authenticateHttpRequest`.

#### AuthenticationState

* Add `AuthenticationState#authenticateAsync`. Include a default implementation that calls the `authenticate` method and then performs a check to determine what result to return. Note that current implementations should all be non-blocking, so there is no need to push the execution to a separate thread.
* Deprecate `AuthenticationState#authenticate`. The preferred method is `AuthenticationState#authenticateAsync`.
* Deprecate `AuthenticationState#isComplete`. This method can be avoided by inferring authentication completeness from the result of `AuthenticationState#authenticateAsync`. When the result is `null`, auth is complete. When it is not `null`, auth is not complete. Since the result of the `authenticateAsync` method is the body delivered to the client, this seems like a reasonable abstraction to make. As a consequence, the `AuthenticationState` is simpler and also avoids certain thread safety issues that might arise when calling `isComplete` from a different thread.

#### AuthenticationDataSource
* Deprecate `AuthenticationDataSource#authenticate`. This method is not called by the Pulsar authentication framework. This needs to be deprecated to prevent confusion for end users seeking to extend the authentication framework. There is no need for an async version of this method.

## Implementation

In addition to updating the above interfaces, we will need to update the consumers of those interfaces. The consumers (and transitive consumers) are currently the following classes: `AuthenticationService`, `AuthenticationFilter`, `WebSocketWebResource`, `AbstractWebSocketHandler`, `ServerCnx`, `ProxyConnection`, and `ServerConnection`.

#### AuthenticationService
It should be trivial to update this class, as the `authenticateHttpRequest` method's return type will be updated to return a `CompletableFuture<String>` instead of a `String`.

#### WebSocketWebResource
This class will require an update to the several endpoints so that they can return asynchronous objects. There is already a pattern in Pulsar for handling this kind of response using `@Suspended final AsyncResponse asyncResponse`, so I expect the implementation to be trivial.

#### AbstractWebSocketHandler
This class has a `checkAuth` method. The creation of a subscription is currently synchronous. I'm not sure how to implement asynchronous responses in websocket handlers. It looks like it should be possible from the following doc: https://webtide.com/jetty-9-updated-websocket-api/. I will need to do a little research to complete this implementation.

#### AuthenticationFilter
This class is an implementation of the `javax.servlet.Filter` interface. We will need to follow the servlet's paradigm for ensuring that asynchronous results are properly handled when determining whether to call `chain.doFilter` after the asynchronous authentication is complete. I will need to do a little research to complete this implementation. 

#### ServerConnection
The `ServerConnection` class is part of the `DiscoveryService`. Instead of updating the implementation, I think it makes more sense to remove the module altogether. I started a thread about this on the mailing list here: https://lists.apache.org/x/thread.html/rc8b796dda2bc1b022e855c7368d832b570967cb1ef29e9cd18e04e97@%3Cdev.pulsar.apache.org%3E. If we do not remove the module, this class will need to be updated as well.

#### ServerCnx and ProxyConnection
Both of these classes rely on netty for method synchronization. Since authentication can happen in another thread, we will need to update these class's state to `State.Connecting` before calling the `authenticateAsync` methods. This will prevent a potential denial of service issue. Then, when the `authenticateAsync` method is called, we will need to register a callback to update the state of the connection class. I believe we will want to schedule this using each class's executor, as follows: `this.ctx().executor()`. By using the class's executor, we won't need to update any values to `volatile`. I'd appreciate confirmation that this interpretation of netty's method is true.

#### Implementations of Interfaces
Once the above changes are implemented, it will make sense to update our implementations of these interfaces to ensure that they implement the new methods.

#### Tests
All of these class changes will have associated test changes.

## Reject Alternatives

On the initial mailing list thread (link in references, below), it was suggested that adding a new async interface would be easier. That would mean that the old, synchronous interface, `AuthenticationProvider` would be deprecated, and we would start with a fresh, new interface. In considering this solution, I decided it was suboptimal because it would lead to the creation of many new translation classes as well as duplicate fields. One such class was described on the mailing list:

```java

class AsyncAuthBridge implements AsyncAuthenticationProvider {
      AuthenticationProvider delegate;
      Executor e;

     CompletableFuture<Boolean> authenticateAsync(AuthenticationDataSource authData) {
         CompletableFuture<Boolean> f = new CompletableFuture<>();
         e.execute(() -> {
               f.complete(delegate.authenticate(authData));
         });
         return f;
    }
}
```

This implementation makes sense, but since existing implementations of the `AuthenticationProvider` interface and the `AuthenticationState` interface are synchronous and should be non-blocking, I don't think we need to worry about pushing their execution to a separate thread.

Further, if we created a new interfaces named `AsyncAuthenticationProvider` and `AsyncAuthenticationState`, I think we'd end up duplicating most of the interface's fields/methods in the original synchronous interfaces because the same logic is still required. We are only changing implementation details by making the authenticate methods run asynchronously.

## References

This addition was first discussed on the dev mailing list here: https://lists.apache.org/x/thread.html/r6c2522ca62242109758586696261cb1f4b4ce8e94ae593fda6e97b99@%3Cdev.pulsar.apache.org%3E.
