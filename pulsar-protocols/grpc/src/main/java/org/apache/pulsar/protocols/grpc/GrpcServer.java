package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
// TODO: Message metadata is internal to Pulsar so must be removed from gRPC
import io.netty.buffer.Unpooled;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.scurrilous.circe.checksum.Crc32cIntChecksum.computeChecksum;
import static com.scurrilous.circe.checksum.Crc32cIntChecksum.resumeChecksum;

public class GrpcServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private Server server;

    public void start(BrokerService service) throws IOException {
        // TODO: replace with configurable port
        int port = 50444;
        server = ServerBuilder.forPort(port)
                .addService(new PulsarGrpcService(service))
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();
        log.info("############# Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            log.error("############### shutting down gRPC server since JVM is shutting down");
            try {
                GrpcServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            log.error("############### gRPC server shut down");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    // TO BE REMOVED : For test
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50444).usePlaintext().build();
        PulsarGrpcServiceGrpc.PulsarGrpcServiceBlockingStub stub = PulsarGrpcServiceGrpc.newBlockingStub(channel);
        String test = stub.hello(SimpleValue.newBuilder().setName("persistent://public/default/my-topic").build()).getName();
        System.out.println(test);

        PulsarGrpcServiceGrpc.PulsarGrpcServiceStub asyncStub = PulsarGrpcServiceGrpc.newStub(channel);
        AtomicReference<StreamObserver<PulsarApi.BaseCommand>> requestRef = new AtomicReference<>();
        StreamObserver<PulsarApi.BaseCommand> responseObserver = new StreamObserver<PulsarApi.BaseCommand>() {
            @Override
            public void onNext(PulsarApi.BaseCommand cmd) {
                PulsarApi.BaseCommand.Type type = cmd.getType();
                switch(type) {
                    case PRODUCER_SUCCESS:
                        PulsarApi.CommandProducerSuccess producerSuccess = cmd.getProducerSuccess();
                        requestRef.get().onNext(getSendCommand(
                            producerSuccess.getProducerName(),
                            producerSuccess.getLastSequenceId() +1,
                            "test message " + (producerSuccess.getLastSequenceId() + 1)
                        ));
                }

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
        StreamObserver<PulsarApi.BaseCommand> request = asyncStub.produce(responseObserver);
        requestRef.set(request);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        request.onCompleted();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static PulsarApi.BaseCommand getSendCommand(String producerName, long sequenceId, String message) {
        MessageMetadata metadata = MessageMetadata.newBuilder()
            .setProducerName(producerName)
            .setSequenceId(sequenceId)
            .setPublishTime(Instant.now().toEpochMilli())
            .build();
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        ByteBuf payload = Unpooled.wrappedBuffer(bytes);
        ByteBuf headersAndPayloadByteBuf = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        ByteString headersAndPayload = ByteString.copyFrom(headersAndPayloadByteBuf.nioBuffer());

        PulsarApi.CommandSend send = PulsarApi.CommandSend.newBuilder()
            .setSequenceId(sequenceId)
            .setProducerId(42L)
            .setHeadersAndPayload(headersAndPayload)
            .build();

        return PulsarApi.BaseCommand.newBuilder()
            .setType(PulsarApi.BaseCommand.Type.SEND)
            .setSend(send)
            .build();
    }

    public static ByteBuf serializeMetadataAndPayload(Commands.ChecksumType checksumType, MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        final short magicCrc32c = 0x0e01;
        final int checksumSize = 4;
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int payloadSize = payload.readableBytes();
        int magicAndChecksumLength = Commands.ChecksumType.Crc32c.equals(checksumType) ? (2 + 4 /* magic + checksumLength*/) : 0;
        boolean includeChecksum = magicAndChecksumLength > 0;
        int headerContentSize = magicAndChecksumLength + 4 + msgMetadataSize; // magicLength +
        // checksumSize + msgMetadataLength +
        // msgMetadataSize
        int checksumReaderIndex = -1;
        int totalSize = headerContentSize + payloadSize;

        ByteBuf metadataAndPayload = PulsarByteBufAllocator.DEFAULT.buffer(totalSize, totalSize);
        try {
            ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(metadataAndPayload);

            //Create checksum placeholder
            if (includeChecksum) {
                metadataAndPayload.writeShort(magicCrc32c);
                checksumReaderIndex = metadataAndPayload.writerIndex();
                metadataAndPayload.writerIndex(metadataAndPayload.writerIndex()
                    + checksumSize); //skip 4 bytes of checksum
            }

            // Write metadata
            metadataAndPayload.writeInt(msgMetadataSize);
            msgMetadata.writeTo(outStream);
            outStream.recycle();
        } catch (IOException e) {
            // This is in-memory serialization, should not fail
            throw new RuntimeException(e);
        }

        // write checksum at created checksum-placeholder
        if (includeChecksum) {
            metadataAndPayload.markReaderIndex();
            metadataAndPayload.readerIndex(checksumReaderIndex + checksumSize);
            int metadataChecksum = computeChecksum(metadataAndPayload);
            int computedChecksum = resumeChecksum(metadataChecksum, payload);
            // set computed checksum
            metadataAndPayload.setInt(checksumReaderIndex, computedChecksum);
            metadataAndPayload.resetReaderIndex();
        }
        metadataAndPayload.writeBytes(payload);

        return metadataAndPayload;
    }
}
