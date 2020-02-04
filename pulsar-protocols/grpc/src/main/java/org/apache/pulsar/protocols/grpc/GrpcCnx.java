package org.apache.pulsar.protocols.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.util.SafeRun;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

public class GrpcCnx implements ServerCnx {

    private static final Logger log = LoggerFactory.getLogger(GrpcCnx.class);

    private final BrokerService service;
    private final SocketAddress remoteAddress;
    private final ServerCallStreamObserver<PulsarApi.BaseCommand> responseObserver;

    // Max number of pending requests per produce RPC
    private static final int MaxPendingSendRequests = 1000;
    private static final int ResumeReadsThreshold = MaxPendingSendRequests / 2;
    private int pendingSendRequest = 0;
    private int nonPersistentPendingMessages = 0;
    private final int MaxNonPersistentPendingMessages;
    private volatile boolean isAutoRead = true;
    private volatile boolean autoReadDisabledRateLimiting = false;
    private final AutoReadAwareOnReadyHandler onReadyHandler = new AutoReadAwareOnReadyHandler();

    public GrpcCnx(BrokerService service, SocketAddress remoteAddress, ServerCallStreamObserver<PulsarApi.BaseCommand> responseObserver) {
        this.service = service;
        this.remoteAddress = remoteAddress;
        this.MaxNonPersistentPendingMessages = service.pulsar().getConfiguration()
            .getMaxConcurrentNonPersistentMessagePerConnection();
        this.responseObserver = responseObserver;
        responseObserver.disableAutoInboundFlowControl();
        responseObserver.setOnReadyHandler(onReadyHandler);
    }

    @Override
    public String getClientVersion() {
        return null;
    }

    @Override
    public SocketAddress clientAddress() {
        return remoteAddress;
    }

    @Override
    public BrokerService getBrokerService() {
        return service;
    }

    @Override
    public boolean isBatchMessageCompatibleVersion() {
        return false;
    }

    @Override
    public String getRole() {
        return null;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return null;
    }

    @Override
    public void removedProducer(Producer producer) {
        responseObserver.onCompleted();
    }

    @Override
    public void closeProducer(Producer producer) {
        responseObserver.onCompleted();
    }

    @Override
    public long getMessagePublishBufferSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public void cancelPublishRateLimiting() {
    }

    @Override
    public void cancelPublishBufferLimiting() {
    }

    @Override
    public void disableCnxAutoRead() {
    }

    public StreamObserver<PulsarApi.BaseCommand> getResponseObserver() {
        return responseObserver;
    }

    public void handleSend(PulsarApi.BaseCommand cmd, Producer producer) {
        PulsarApi.CommandSend send = cmd.getSend();
        ByteBuffer buffer = send.getHeadersAndPayload().asReadOnlyByteBuffer();
        ByteBuf headersAndPayload = Unpooled.wrappedBuffer(buffer);

        if (producer.isNonPersistentTopic()) {
            // avoid processing non-persist message if reached max concurrent-message limit
            if (nonPersistentPendingMessages > MaxNonPersistentPendingMessages) {
                final long sequenceId = send.getSequenceId();
                final long highestSequenceId = send.getHighestSequenceId();
                service.getTopicOrderedExecutor().executeOrdered(
                    producer.getTopic().getName(),
                    SafeRun.safeRun(() -> responseObserver.onNext(Commands.newSendReceipt(sequenceId, highestSequenceId, -1, -1)))
                );
                producer.recordMessageDrop(send.getNumMessages());
                return;
            } else {
                nonPersistentPendingMessages++;
            }
        }

        startSendOperation(producer);

        // Persist the message
        if (send.hasHighestSequenceId() && send.getSequenceId() <= send.getHighestSequenceId()) {
            producer.publishMessage(producer.getProducerId(), send.getSequenceId(), send.getHighestSequenceId(),
                headersAndPayload, send.getNumMessages());
        } else {
            producer.publishMessage(producer.getProducerId(), send.getSequenceId(), headersAndPayload, send.getNumMessages());
        }
        onMessageHandled();
    }

    private void startSendOperation(Producer producer) {
        boolean isPublishRateExceeded = producer.getTopic().isPublishRateExceeded();
        if (++pendingSendRequest == MaxPendingSendRequests || isPublishRateExceeded) {
            // When the quota of pending send requests is reached, stop reading from channel to cause backpressure on
            // client connection
            isAutoRead = false;
            autoReadDisabledRateLimiting = isPublishRateExceeded;
        }
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {
        if (--pendingSendRequest == ResumeReadsThreshold) {
            // Resume producer
            isAutoRead = true;
            if (responseObserver.isReady()) {
                responseObserver.request(1);
            }
        }
        if (isNonPersistentTopic) {
            nonPersistentPendingMessages--;
        }
    }

    @Override
    public void enableCnxAutoRead() {
        // we can add check (&& pendingSendRequest < MaxPendingSendRequests) here but then it requires
        // pendingSendRequest to be volatile and it can be expensive while writing. also this will be called on if
        // throttling is enable on the topic. so, avoid pendingSendRequest check will be fine.
        if (!isAutoRead && autoReadDisabledRateLimiting) {
            // Resume reading from socket if pending-request is not reached to threshold
            isAutoRead = true;
            // triggers channel read
            if (responseObserver.isReady()) {
                responseObserver.request(1);
            }
            autoReadDisabledRateLimiting = false;
        }
    }

    public void onMessageHandled() {
        if (responseObserver.isReady() && isAutoRead) {
            responseObserver.request(1);
        } else {
            onReadyHandler.wasReady = false;
        }
    }

    class AutoReadAwareOnReadyHandler implements Runnable {
        // Guard against spurious onReady() calls caused by a race between onNext() and onReady(). If the transport
        // toggles isReady() from false to true while onNext() is executing, but before onNext() checks isReady(),
        // request(1) would be called twice - once by onNext() and once by the onReady() scheduled during onNext()'s
        // execution.
        private boolean wasReady = false;

        @Override
        public void run() {
            if (responseObserver.isReady() && !wasReady) {
                wasReady = true;
                if(isAutoRead) {
                    responseObserver.request(1);
                }
            }
        }
    }
}
