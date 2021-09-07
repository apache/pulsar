package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-impl")
public class ProducerCloseTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10_000)
    public void testProducerCloseCallback() throws Exception {
        initClient();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerClose")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        final TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
        final TypedMessageBuilder<byte[]> value = messageBuilder.value("test-msg".getBytes(StandardCharsets.UTF_8));
        producer.getClientCnx().channel().config().setAutoRead(false);
        final CompletableFuture<MessageId> completableFuture = value.sendAsync();
        producer.closeAsync();
        final CommandSuccess commandSuccess = new CommandSuccess();
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        commandSuccess.setRequestId(clientImpl.newRequestId() -1);
        producer.getClientCnx().handleSuccess(commandSuccess);
        Thread.sleep(3000);
        Assert.assertEquals(completableFuture.isDone(), true);
    }

    private void initClient() throws PulsarClientException {
        pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .build();
    }

}
