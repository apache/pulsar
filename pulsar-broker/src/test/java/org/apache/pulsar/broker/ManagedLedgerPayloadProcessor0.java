package org.apache.pulsar.broker;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;

public class ManagedLedgerPayloadProcessor0 implements ManagedLedgerPayloadProcessor {

    private final AtomicInteger counter = new AtomicInteger(0);
    private final int failAt = 4;

    @Override
    public Processor inputProcessor() {
        return new Processor() {
            @Override
            public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                if (counter.incrementAndGet() == failAt) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("Failed to process input payload");
                }
                return inputPayload.retainedDuplicate();
            }

            @Override
            public void release(ByteBuf processedPayload) {
                processedPayload.release();
            }
        };
    }
}
