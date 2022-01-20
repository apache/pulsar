package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Progress;

import java.util.concurrent.CompletableFuture;

public class ProgressSendCallback implements SendCallback{
    private Progress progress;

    public void setProgress(Progress progress) {
        this.progress = progress;
    }

    public Progress getProgress() {
        return this.progress;
    }

    @Override
    public void sendComplete(Exception e) {

    }

    @Override
    public void addCallback(MessageImpl<?> msg, SendCallback scb) {

    }

    @Override
    public SendCallback getNextSendCallback() {
        return null;
    }

    @Override
    public MessageImpl<?> getNextMessage() {
        return null;
    }

    @Override
    public CompletableFuture<MessageId> getFuture() {
        return null;
    }
}
