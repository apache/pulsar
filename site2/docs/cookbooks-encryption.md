---
id: cookbooks-encryption
title: Configure end-to-end encryption
sidebar_label: "Configure end-to-end encryption"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

[Pulsar encryption](security-encryption.md) allows clients to encrypt messages at producers and decrypt messages at consumers.

## Prerequisites

* Pulsar Java/Python/C++/Node.js client 2.7.1 or later versions.
* Pulsar Go client 0.6.0 or later versions.

## Configure end-to-end encryption

1. Create both public and private key pairs.
   * ECDSA（for Java and Go clients)
     ```shell
     openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
     openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem
     ```

   * RSA (for Python, C++ and Node.js clients)
     ```shell
     openssl genrsa -out test_rsa_privkey.pem 2048
     openssl rsa -in test_rsa_privkey.pem -pubout -outform pkcs8 -out test_rsa_pubkey.pem
     ```

2. Configure a `CryptoKeyReader` on producers, consumers or readers.

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Go","value":"Go"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```java
   PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
   String topic = "persistent://my-tenant/my-ns/my-topic";
   // RawFileKeyReader is just an example implementation that's not provided by Pulsar
   CryptoKeyReader keyReader = new RawFileKeyReader("test_ecdsa_pubkey.pem", "test_ecdsa_privkey.pem");

   Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .cryptoKeyReader(keyReader)
        .addEncryptionKey("myappkey")
        .create();

   Consumer<byte[]> consumer = pulsarClient.newConsumer()
        .topic(topic)
        .subscriptionName("my-subscriber-name")
        .cryptoKeyReader(keyReader)
        .subscribe();

   Reader<byte[]> reader = pulsarClient.newReader()
        .topic(topic)
        .startMessageId(MessageId.earliest)
        .cryptoKeyReader(keyReader)
        .create();
   ```

   </TabItem>
   <TabItem value="Python">

   ```python
   from pulsar import Client, CryptoKeyReader

   client = Client('pulsar://localhost:6650')
   topic = 'my-topic'
   # CryptoKeyReader is a built-in implementation that reads public key and private key from files
   key_reader = CryptoKeyReader('test_rsa_pubkey.pem', 'test_rsa_privkey.pem')

   producer = client.create_producer(
       topic=topic,
       encryption_key='myappkey',
       crypto_key_reader=key_reader
   )

   consumer = client.subscribe(
       topic=topic,
       subscription_name='my-subscriber-name',
       crypto_key_reader=key_reader
   )

   reader = client.create_reader(
       topic=topic,
       start_message_id=MessageId.earliest,
       crypto_key_reader=key_reader
   )

   client.close()
   ```

   </TabItem>
   <TabItem value="C++">

   ```cpp
   Client client("pulsar://localhost:6650");
   std::string topic = "persistent://my-tenant/my-ns/my-topic";
   // DefaultCryptoKeyReader is a built-in implementation that reads public key and private key from files
   auto keyReader = std::make_shared<DefaultCryptoKeyReader>("test_rsa_pubkey.pem", "test_rsa_privkey.pem");

   Producer producer;
   ProducerConfiguration producerConf;
   producerConf.setCryptoKeyReader(keyReader);
   producerConf.addEncryptionKey("myappkey");
   client.createProducer(topic, producerConf, producer);

   Consumer consumer;
   ConsumerConfiguration consumerConf;
   consumerConf.setCryptoKeyReader(keyReader);
   client.subscribe(topic, "my-subscriber-name", consumerConf, consumer);

   Reader reader;
   ReaderConfiguration readerConf;
   readerConf.setCryptoKeyReader(keyReader);
   client.createReader(topic, MessageId::earliest(), readerConf, reader);
   ```

   </TabItem>
   <TabItem value="Go">

   ```go
   client, err := pulsar.NewClient(pulsar.ClientOptions{
	 URL: "pulsar://localhost:6650",
   })
   if err != nil {
	   log.Fatal(err)
   }

   defer client.Close()

   topic := "persistent://my-tenant/my-ns/my-topic"
   keyReader := crypto.NewFileKeyReader("test_ecdsa_pubkey.pem", "test_ecdsa_privkey.pem")
   producer, err := client.CreateProducer(pulsar.ProducerOptions{
	   Topic: topic,
	   Encryption: &pulsar.ProducerEncryptionInfo{
	   	KeyReader: keyReader,
	   	Keys:      []string{"myappkey"},
	   },
   })
   if err != nil {
   	log.Fatal(err)
   }
   defer producer.Close()

   consumer, err := client.Subscribe(pulsar.ConsumerOptions{
	   Topic:            topic,
	   SubscriptionName: "my-subscriber-name",
	   Decryption: &pulsar.MessageDecryptionInfo{
		   KeyReader: keyReader,
	   },
   })
   if err != nil {
	   log.Fatal(err)
   }
   defer consumer.Close()

   reader, err := client.CreateReader(pulsar.ReaderOptions{
	   Topic: topic,
	   Decryption: &pulsar.MessageDecryptionInfo{
		   KeyReader: keyReader,
	   },
   })
   if err != nil {
	   log.Fatal(err)
   }
   defer reader.Close()
   ```

   </TabItem>
   <TabItem value="Node.js">

   ```javascript
   const Pulsar = require('pulsar-client');

   const topic = 'persistent://my-tenant/my-ns/my-topic';

   (async () => {
   // Create a client
   const client = new Pulsar.Client({
       serviceUrl: 'pulsar://localhost:6650',
       operationTimeoutSeconds: 30,
   });

   // Create a producer
   const producer = await client.createProducer({
       topic: topic,
       sendTimeoutMs: 30000,
       batchingEnabled: true,
       publicKeyPath: "test_rsa_pubkey.pem",
       encryptionKey: "encryption-key"
   });

   // Create a consumer
   const consumer = await client.subscribe({
       topic: topic,
       subscription: 'my-subscriber-name',
       subscriptionType: 'Shared',
       ackTimeoutMs: 10000,
       privateKeyPath: "test_rsa_privkey.pem"
   });
   await consumer.close();
   await producer.close();
   await client.close();
   })();
   ```

   </TabItem>
   </Tabs>
   ````

3. Optional: customize the `CryptoKeyReader` implementation.
   
   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Go","value":"Go"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```java
   class RawFileKeyReader implements CryptoKeyReader {

    String publicKeyFile = "";
    String privateKeyFile = "";

    RawFileKeyReader(String pubKeyFile, String privKeyFile) {
        publicKeyFile = pubKeyFile;
        privateKeyFile = privKeyFile;
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(publicKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read public key from file " + publicKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        try {
            keyInfo.setKey(Files.readAllBytes(Paths.get(privateKeyFile)));
        } catch (IOException e) {
            System.out.println("ERROR: Failed to read private key from file " + privateKeyFile);
            e.printStackTrace();
        }
        return keyInfo;
    }
   }
   ```

   </TabItem>
   <TabItem value="Python">

   Currently, customizing the `CryptoKeyReader` implementation is not supported in Python. However, you can use the default implementation by specifying the path of the private key and public keys.

   </TabItem>
   <TabItem value="C++">

   ```cpp
   class CustomCryptoKeyReader : public CryptoKeyReader {
    public:
    Result getPublicKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                        EncryptionKeyInfo& encKeyInfo) const override {
        // TODO
        return ResultOk;
    }

    Result getPrivateKey(const std::string& keyName, std::map<std::string, std::string>& metadata,
                        EncryptionKeyInfo& encKeyInfo) const override {
        // TODO
        return ResultOk;
    }
   };
   ```

   </TabItem>
   <TabItem value="Go">

   ```go
   type CustomKeyReader struct {
	   publicKeyPath  string
	   privateKeyPath string
   }

   func (c *CustomKeyReader) PublicKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	   keyInfo := &EncryptionKeyInfo{}
	   // TODO
	   return keyInfo, nil
   }

   // PrivateKey read private key from the given path
   func (c *CustomKeyReader) PrivateKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	   keyInfo := &EncryptionKeyInfo{}
	   // TODO
	   return keyInfo, nil
   }
   ```

   </TabItem>
   <TabItem value="Node.js">

   Currently, customizing the `CryptoKeyReader` implementation is not supported in Python. However, you can use the default implementation by specifying the path of the private key and public keys.

   </TabItem>
   </Tabs>
   ````

## Encrypt a message with multiple keys

:::note

This is only available for Java clients.

:::

You can encrypt a message with more than one key. Producers add all such keys to the config and consumers can decrypt the message as long as they have access to at least one of the keys. Any one of the keys used for encrypting the message is sufficient to decrypt the message. 

For example, encrypt the messages using 2 keys (`myapp.messagekey1` and `myapp.messagekey2`):

```java
PulsarClient.newProducer().addEncryptionKey("myapp.messagekey1").addEncryptionKey("myapp.messagekey2");
```