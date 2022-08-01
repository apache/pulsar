---
id: functions-deploy-cluster-encryption
title: Enable end-to-end-encryption
sidebar_label: "Enable end-to-end-encryption"
---

To perform end-to-end [encryption](security-encryption.md), you can specify `--producer-config` and `--input-specs` in the [`pulsar-admin`](/tools/pulsar-admin/) CLI with the public and private key pair configured by the application. Only the consumers with a valid key can decrypt the encrypted messages. 

The encryption/decryption relevant configuration [`CryptoConfig`](functions-cli.md) is included in both `ProducerConfig` and `inputSpecs`. The specific configurable fields about `CryptoConfig` are as follows:

```text

public class CryptoConfig {
    private String cryptoKeyReaderClassName;
    private Map<String, Object> cryptoKeyReaderConfig;

    private String[] encryptionKeys;
    private ProducerCryptoFailureAction producerCryptoFailureAction;

    private ConsumerCryptoFailureAction consumerCryptoFailureAction;
}

```

- `producerCryptoFailureAction` defines the action that a producer takes if it fails to encrypt the data. Available options are `FAIL` or `SEND`.
- `consumerCryptoFailureAction` defines the action that a consumer takes if it fails to decrypt the recieved data. Available options are `FAIL`, `DISCARD`, or `CONSUME`.

For more information about these options, refer to [producer configurations](client-libraries-java.md#configure-producer.md) and [consumer configurations](client-libraries-java.md#configure-consumer).
