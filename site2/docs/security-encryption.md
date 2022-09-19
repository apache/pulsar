---
id: security-encryption
title: Pulsar Encryption
sidebar_label: "End-to-End Encryption"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Applications can use Pulsar end-to-end encryption (E2EE) to encrypt messages on the producer side and decrypt messages on the consumer side. You can use the public and private key pair that the application configures to perform encryption and decryption. Only the consumers with a valid key can decrypt the encrypted messages.

## How it works

Pulsar uses a dynamically generated symmetric AES key to encrypt messages (data). You can use the application-provided ECDSA (Elliptic Curve Digital Signature Algorithm) or RSA (Rivest–Shamir–Adleman) key pair to encrypt the AES key (data key), so you do not have to share the secret with everyone.

The application configures the producer with the public key for encryption. You can use this key to encrypt the AES data key. The encrypted data key is sent as part of the message header. Only entities with the private key (in this case the consumer) can decrypt the data key which is used to decrypt the message.

The following figure illustrates how Pulsar encrypts messages on the producer side and decrypts messages on the consumer side.

![alt text](/assets/pulsar-encryption.svg "Pulsar Encryption Process")

If produced messages are consumed across application boundaries, you need to ensure that consumers in other applications have access to one of the private keys that can decrypt the messages. You can do this in two ways:
1. The consumer application provides you access to the public key, which you add to your producer keys.
2. You grant access to one of the private keys from the pairs that the producer uses. 

:::tip

* Pulsar does not store the encryption key anywhere in the Pulsar service. If you lose or delete the private key, your message is irretrievably lost and unrecoverable.
* Pulsar generates a new AES data key every 4 hours or after publishing a certain number of messages. Producers fetch the asymmetric public key every 4 hours by calling `CryptoKeyReader.getPublicKey()` to retrieve the latest version.

:::


## Get started

Pulsar encryption allows applications to encrypt messages on the producer side and decrypt messages on the consumer side. See [cookbook](cookbooks-encryption.md) for detailed instructions.


## Troubleshoot

* Producer/Consumer loses access to the key
  * Producer action fails to indicate the cause of the failure. Application has the option to proceed with sending unencrypted messages in such cases. Call `PulsarClient.newProducer().cryptoFailureAction(ProducerCryptoFailureAction)` to control the producer behavior. The default behavior is to fail the request.
  * If consumption fails due to decryption failure or missing keys in the consumer, the application has the option to consume the encrypted message or discard it. Call `PulsarClient.newConsumer().cryptoFailureAction(ConsumerCryptoFailureAction)` to control the consumer behavior. The default behavior is to fail the request. Application is never able to decrypt the messages if the private key is permanently lost.
* Batch messaging
  * If decryption fails and the message contains batch messages, client is not able to retrieve individual messages in the batch, hence message consumption fails even if `cryptoFailureAction()` is set to `ConsumerCryptoFailureAction.CONSUME`.
* If decryption fails, the message consumption stops and the application notices backlog growth in addition to decryption failure messages in the client log. If the application does not have access to the private key to decrypt the message, the only option is to skip or discard backlogged messages.
