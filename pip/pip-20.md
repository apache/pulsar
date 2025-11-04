# PIP-20: Mechanism to revoke TLS authentication

- **Status**: Proposed
- **Author**: [Ivan Kelly](https://github.com/ivankelly)
- **Pull Request**: -
- **Mailing List discussion**: https://lists.apache.org/thread.html/ac70badf3648cb4287a483b9ee75d7cf599126bba9e45f30acbb7ba4@%3Cdev.pulsar.apache.org%3E

# Motivation

TLS authentication allows clients to authenticate with the Pulsar service using certs which have been signed by a centralized certificate authority(CA). There is no just-in-time validation of the cert. If the cert is signed by the CA, then we accept it.

If a cert is stolen or compromised, we currently have no way of revoking access for that cert.

SSL generally deals with this using [OSCP](https://en.wikipedia.org/wiki/Online_Certificate_Status_Protocol) or [CRL](https://en.wikipedia.org/wiki/Certificate_revocation_list). For OSCP, the CA has to provide an endpoint against which certificates can be checked. For CRLs, the CA has to provide a endpoint from which a time limited list of revoked certs can be downloaded. In both cases, this is not suitable for Pulsar.

Another shortcoming of these schemes is that they only check the validity on negotiation. Pulsar can have very long lived connections. If a client is compromised while holding a connection open, we need to be able to kick them.

This PIP proposes a adhoc mechanism, where we store a list of revoked keys in zookeeper, and use this to validate new and existing connections.

# Design

For the purposes of this mechanism, a cert will be identified by its [Subject Key Identifier](https://tools.ietf.org/html/rfc5280#section-4.2.1.2).

You can view the subject key identifier of a cert with the following command.
```
openssl x509 -in admin.cert.pem -noout -text
```

To revoke a certs access, there is a CLI tool.
```bash
bin/pulsar-admin brokers revoke-tls-cert \
    13:33:30:00:38:9F:60:5E:F4:12:4B:B0:5E:DF:EA:A6:AD:BD:64:54
```

This will add the subject key identifier to the broker dynamic configuration under the key, tls-revoked-certs. Each broker will listen for changes on this dynamic configuration key.

When a client connects and attempts TLS auth, the server will check the authenticating cert against this list. When the list changes, each server will go through the list of connected authenticated clients and check the cert in use against this list.

# Changes

The following changes are needed.

1. A cli for adding subject key identifiers to the dynamic configuration.
2. SecurityUtility.java should wrap all TrustManagers in a wrapper, that, after calling the parent method, checks the subject key identifier against the revoked list. The trust managers are used by both jetty and netty connections for negotiation (see [demo](https://github.com/ivankelly/incubator-pulsar/commit/216c0c9ea22fb8431c2c5f1c9f597183ee400981)).
3. Each service watches the list, and on update checks all existing connections:
    - For netty, each TLS channel should be added to a ChannelGroup which we can iterate over. To check a channel, the peer cert can be retrieved by ```channel.getPipeline().getHandler(SslHandler.class).getEngine().getSession().getPeerCertificateChain()```.
    - For jetty, all connections can be retrieved with ```getConnectedEndpoint()```. The ```Connection``` objects can be accessed from the returned list, and checked if they are instances of ```SSLConnection```. Once you have an ```SSLConnection```, the peer cert can be accessed via ```sslconn.getEngine().getSession().getPeerCertificateChain()```.
