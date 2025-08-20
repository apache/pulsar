# PIP-432: Add isEncrypted field to EncryptionContext

# Background knowledge

Apache Pulsar supports client-side encryption where messages can be encrypted by producers and decrypted by consumers. When a message is encrypted, Pulsar includes an `EncryptionContext` with each message that contains encryption metadata such as:

- **Encryption keys**: The encrypted data encryption keys used for message encryption
- **Encryption parameters**: Additional parameters like initialization vectors (IV)
- **Encryption algorithm**: The algorithm used (e.g., RSA, ECDSA)
- **Compression information**: Whether compression was applied before encryption

**Key concepts:**
- **EncryptionContext**: A metadata object attached to encrypted messages containing encryption-related information
- **CryptoKeyReader**: An interface that provides public/private keys for encryption/decryption operations
- **ConsumerCryptoFailureAction**: Determines how consumers handle decryption failures:
  - `FAIL`: Fail message consumption (default)
  - `DISCARD`: Silently discard the message
  - `CONSUME`: Deliver the encrypted message to the application

Currently, when `ConsumerCryptoFailureAction.CONSUME` is configured, consumers can receive encrypted messages even when decryption fails (e.g., missing private key, mismatched keys). However, applications have no way to determine whether the received message was successfully decrypted or is still encrypted.

# Motivation

Applications using Pulsar's encryption feature with `ConsumerCryptoFailureAction.CONSUME` need to determine whether received messages were successfully decrypted or if decryption failed. This is essential for:

1. **Error handling**: Applications need to know when they receive encrypted (undecrypted) data to handle it appropriately
2. **Monitoring**: Applications want to track decryption success/failure rates for monitoring and alerting
3. **Manual decryption**: When automatic decryption fails, applications may want to attempt manual decryption using the EncryptionContext
4. **Security compliance**: Applications need to ensure they're not inadvertently processing encrypted data as plain text

**Current situation:**
- Consumers with `CONSUME` action receive messages regardless of decryption success
- No programmatic way to distinguish between successfully decrypted and failed decryption messages
- Applications must implement workarounds to detect encrypted vs. decrypted content

**Use cases this solves:**
1. Consumer without private key configured → should know decryption failed
2. Consumer with mismatched private key → should know decryption failed  
3. Consumer with correct private key → should know decryption succeeded

# Goals

## In Scope

- Add an `isEncrypted` boolean field to the `EncryptionContext` class
- Update consumer decryption logic to populate this field correctly
- Ensure the field accurately reflects decryption status for all encryption scenarios
- Maintain backward compatibility with existing applications
- Update existing encryption tests to verify the new functionality

## Out of Scope

- Changes to encryption/decryption algorithms or protocols
- Modifications to `ConsumerCryptoFailureAction` behavior
- Performance improvements to encryption/decryption operations
- New encryption features or capabilities
- Changes to producer-side encryption logic

# High Level Design

The solution adds a simple boolean field `isEncrypted` to the existing `EncryptionContext` class. This field is set during message processing in the consumer:

1. **Successful decryption**: When a consumer successfully decrypts a message, `isEncrypted` is set to `false`
2. **Failed decryption**: When decryption fails (missing key, wrong key, etc.) but `ConsumerCryptoFailureAction.CONSUME` is configured, `isEncrypted` is set to `true`
3. **No encryption**: For non-encrypted messages, no `EncryptionContext` is created (existing behavior)

The field is populated in the consumer's message creation logic, specifically in the `createEncryptionContext()` method where the decryption success/failure status is already known.

Applications can then check this field to determine if the received message payload is encrypted or decrypted:

```java
Message<byte[]> message = consumer.receive();
Optional<EncryptionContext> ctx = message.getEncryptionCtx();
if (ctx.isPresent()) {
    if (ctx.get().isEncrypted()) {
        // Handle encrypted message - decryption failed
        handleEncryptedMessage(message, ctx.get());
    } else {
        // Handle decrypted message - decryption succeeded
        handleDecryptedMessage(message);
    }
}
```

## Public-facing Changes

### Public API

**New method available via Lombok-generated getter:**
```java
public boolean isEncrypted()
```

**Usage pattern:**
```java
Message<T> message = consumer.receive();
Optional<EncryptionContext> encryptionCtx = message.getEncryptionCtx();
if (encryptionCtx.isPresent()) {
    boolean encrypted = encryptionCtx.get().isEncrypted();
    if (encrypted) {
        // Message is encrypted (decryption failed)
    } else {
        // Message is decrypted (decryption succeeded)
    }
}
```

**Breaking Changes:** None - this is a purely additive change.

# Backward & Forward Compatibility

No compatibility concerns.

## Downgrade / Rollback

Rolling back to a previous Pulsar version:
1. Applications using `isEncrypted()` will get compilation errors - this is expected
2. Remove calls to `isEncrypted()` from application code
3. Downgrade Pulsar client library
4. No data loss or corruption risk