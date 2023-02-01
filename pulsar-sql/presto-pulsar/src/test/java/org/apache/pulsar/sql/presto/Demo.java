package org.apache.pulsar.sql.presto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Demo {

    @Data
    @NoArgsConstructor
    @ToString
    static class User {
        private String name;
        private Integer age;
    }

    public void sendData() throws Exception {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String topic = "user";
        Producer<User> producer = pulsarClient.newProducer(Schema.AVRO(User.class))
                .topic(topic)
                .create();

        Consumer<User> consumer = pulsarClient.newConsumer(Schema.AVRO(User.class))
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.close();

        for (int i = 0; i < 10; i++) {
            User user = new User();
            user.setName("user-" + i);
            user.setAge(10 + i);
            producer.newMessage().value(user).send();
        }

        producer.close();
        pulsarClient.close();
    }

    public void sendDataWithRepeatedKey() throws Exception {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String topic = "pt-10";
        Producer<User> producer = pulsarClient.newProducer(Schema.AVRO(User.class))
                .topic(topic)
                .create();

        Consumer<User> consumer = pulsarClient.newConsumer(Schema.AVRO(User.class))
                .topic(topic)
                .subscriptionName("sub")
                .readCompacted(true)
                .subscribe();
        consumer.close();

        for (int i = 0; i < 100; i++) {
            User user = new User();
            user.setName("user-" + i);
            user.setAge(10 + i);
            producer.newMessage().key("" + i % 10).value(user).send();
        }

        producer.close();
        pulsarClient.close();
    }

    public void readCompactedData() throws Exception {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        String topic = "pt-10";
        Consumer<User> consumer = pulsarClient.newConsumer(Schema.AVRO(User.class))
                .topic(topic)
                .subscriptionName("sub")
                .readCompacted(true)
                .subscribe();

        long receiveCount = 0;
        while (true) {
            Message<User> message = consumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                break;
            }
            receiveCount++;
            System.out.println("receive message " + receiveCount
                    + ", key: " + message.getKey()
                    + ", value: " + message.getValue().toString()
                    + ", messageId: " + message.getMessageId());
        }

        consumer.close();
        pulsarClient.close();
    }

    public static void main(String[] args) throws Exception {
        Demo demo = new Demo();
        demo.readCompactedData();
    }

}
