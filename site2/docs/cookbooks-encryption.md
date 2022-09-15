---
id: cookbooks-encryption
title: Pulsar Encryption
sidebar_label: "Encryption"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

Pulsar encryption allows clients to encrypt messages at producers and decrypt messages at consumers.

## Prerequisites

* Pulsar Python/Node.js client 2.7.1 or later versions.
* Pulsar Java//C++/Go client 2.7.1 or later versions.

## Steps

1. Configure key paths for producers and consumers.

   ```properties
   publicKeyPath: "./public.pem"
   privateKeyPath: "./private.pem"
   ```

2. Create both public and private key pairs.
   * ECDSA（for Java clients only)
     ```shell
     openssl ecparam -name secp521r1 -genkey -param_enc explicit -out test_ecdsa_privkey.pem
     openssl ec -in test_ecdsa_privkey.pem -pubout -outform pem -out test_ecdsa_pubkey.pem
     ```

   * RSA (for Python, C++ and Node.js clients)
     ```shell
     openssl genrsa -out private.pem 2048
     openssl rsa -in private.pem -pubout -out public.pem
     ```

3. Create a producer to send encrypted messages.

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```java

   ```

   </TabItem>
   <TabItem value="Python">

   ```python
   import pulsar

   publicKeyPath = "./public.pem"
   privateKeyPath = ""
   crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
   client = pulsar.Client('pulsar://localhost:6650')
   producer = client.create_producer(topic='encryption', encryption_key='encryption', crypto_key_reader=crypto_key_reader)
   producer.send('encryption message'.encode('utf8'))
   print('sent message')
   producer.close()
   client.close()
   ```

   </TabItem>
   <TabItem value="C++">

   ```cpp

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```javascript
   const Pulsar = require('pulsar-client');

   (async () => {
     // Create a client
     const client = new Pulsar.Client({
       serviceUrl: 'pulsar://localhost:6650',
       operationTimeoutSeconds: 30,
     });

     // Create a producer
     const producer = await client.createProducer({
       topic: 'persistent://public/default/my-topic',
       sendTimeoutMs: 30000,
       batchingEnabled: true,
       publicKeyPath: "./public.pem",
       encryptionKey: "encryption-key"
     });

     console.log(producer.ProducerConfig)
     // Send messages
     for (let i = 0; i < 10; i += 1) {
       const msg = `my-message-${i}`;
       producer.send({
         data: Buffer.from(msg),
       });
       console.log(`Sent message: ${msg}`);
     }
     await producer.flush();

     await producer.close();
     await client.close();
   })();
   ```

   </TabItem>
   </Tabs>
   ````

4. Create a consumer to receive encrypted messages.
   
   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```java

   ```

   </TabItem>
   <TabItem value="Python">

   ```python
   import pulsar

   publicKeyPath = ""
   privateKeyPath = "./private.pem"
   crypto_key_reader = pulsar.CryptoKeyReader(publicKeyPath, privateKeyPath)
   client = pulsar.Client('pulsar://localhost:6650')
   consumer = client.subscribe(topic='encryption', subscription_name='encryption-sub', crypto_key_reader=crypto_key_reader)
   msg = consumer.receive()
   print("Received msg '{}' id = '{}'".format(msg.data(), msg.message_id()))
   consumer.close()
   client.close()
   ```

   </TabItem>
   <TabItem value="C++">

   ```cpp

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```javascript
   const Pulsar = require('pulsar-client');

   (async () => {
     // Create a client
     const client = new Pulsar.Client({
       serviceUrl: 'pulsar://172.25.0.3:6650',
       operationTimeoutSeconds: 30
     });

     // Create a consumer
     const consumer = await client.subscribe({
       topic: 'persistent://public/default/my-topic',
       subscription: 'sub1',
       subscriptionType: 'Shared',
       ackTimeoutMs: 10000,
       privateKeyPath: "./private.pem"
     });

     console.log(consumer)
     // Receive messages
     for (let i = 0; i < 10; i += 1) {
       const msg = await consumer.receive();
       console.log(msg.getData().toString());
       consumer.acknowledge(msg);
     }

     await consumer.close();
     await client.close();
   })();
   ```

   </TabItem>
   </Tabs>
   ````

5. Run the consumer to receive encrypted messages.

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```shell

   ```

   </TabItem>
   <TabItem value="Python">

   ```shell
   python consumer.py
   ```

   </TabItem>
   <TabItem value="C++">

   ```shell

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```shell
   node consumer.js
   ```

   </TabItem>
   </Tabs>
   ````

6. In a new terminal tab, run the producer to produce encrypted messages.

   **Input**

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```shell

   ```

   </TabItem>
   <TabItem value="Python">

   ```shell
   python producer.py
   ```

   </TabItem>
   <TabItem value="C++">

   ```shell

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```shell
   node producer.js
   ```

   </TabItem>
   </Tabs>
   ````

   Now you can see the producer sends messages and the consumer receives messages successfully.

   **Output**

   This is the output from the producer side.

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```shell

   ```

   </TabItem>
   <TabItem value="Python">

   ```shell
   sent message
   ```

   </TabItem>
   <TabItem value="C++">

   ```shell

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```shell
   Sent message: my-message-0
   Sent message: my-message-1
   Sent message: my-message-2
   Sent message: my-message-3
   Sent message: my-message-4
   Sent message: my-message-5
   Sent message: my-message-6
   Sent message: my-message-7
   Sent message: my-message-8
   Sent message: my-message-9
   ```

   </TabItem>
   </Tabs>
   ````

   This is the output from the consumer side.

   ````mdx-code-block
   <Tabs groupId="lang-choice"
     defaultValue="Java"
     values={[{"label":"Java","value":"Java"},{"label":"Python","value":"Python"},{"label":"C++","value":"C++"},{"label":"Node.js","value":"Node.js"}]}>
   <TabItem value="Java">

   ```shell

   ```

   </TabItem>
   <TabItem value="Python">

   ```shell
   Received msg 'encryption message' id = '(0,0,-1,-1)'
   ```

   </TabItem>
   <TabItem value="C++">

   ```shell

   ```

   </TabItem>
   <TabItem value="Node.js">

   ```shell
   my-message-0
   my-message-1
   my-message-2
   my-message-3
   my-message-4
   my-message-5
   my-message-6
   my-message-7
   my-message-8
   my-message-9
   ```

   </TabItem>
   </Tabs>
   ````
