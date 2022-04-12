package org.apache.pulsar.broker.service.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.broker.service.schema.SchemaRegistryServiceImpl.Functions.convertToDomainType;
import static org.apache.pulsar.broker.service.schema.SchemaRegistryServiceImpl.Functions.toMap;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.proto.SchemaRegistryFormat;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;

public class MockSchemaRegistry extends SchemaRegistryServiceImpl {
    ServiceConfiguration serviceConfiguration;
    SchemaStorage schemaStorage;

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        CompletableFuture<SchemaAndMetadata> result = new CompletableFuture<>();
        CompletableFuture<List<CompletableFuture<StoredSchema>>> storedSchemaCompletableFuture =
                this.schemaStorage.getAll(schemaId);

        try {
            int version = storedSchemaCompletableFuture.get().size() - 1;
            SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.parseFrom(
                    storedSchemaCompletableFuture.get().get(version).get().data);
            SchemaData schemaData = SchemaData.builder()
                    .user(info.getUser())
                    .type(convertToDomainType(info.getType()))
                    .data(info.getSchema().toByteArray())
                    .isDeleted(info.getDeleted())
                    .props(toMap(info.getPropsList()))
                    .build();
            result.complete(new SchemaAndMetadata(schemaId, schemaData,
                    storedSchemaCompletableFuture.get().get(version).get().version));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
        CompletableFuture<SchemaAndMetadata> result = new CompletableFuture<>();
        int versionInt = ByteBuffer.wrap(version.bytes()).getInt();
        CompletableFuture<List<CompletableFuture<StoredSchema>>> storedSchemaCompletableFuture =
                this.schemaStorage.getAll(schemaId);

        try {
            SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.parseFrom(
                    storedSchemaCompletableFuture.get().get(versionInt).get().data);
            SchemaData schemaData = SchemaData.builder()
                    .user(info.getUser())
                    .type(convertToDomainType(info.getType()))
                    .data(info.getSchema().toByteArray())
                    .isDeleted(info.getDeleted())
                    .props(toMap(info.getPropsList()))
                    .build();
            result.complete(new SchemaAndMetadata(schemaId, schemaData,
                    storedSchemaCompletableFuture.get().get(versionInt).get().version));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        return result;
    }

    @Override
    public CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> getAllSchemas(String schemaId) {

        CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> finalResult = new CompletableFuture<>();
        List<CompletableFuture<SchemaAndMetadata>> result = new ArrayList<>();
        CompletableFuture<List<CompletableFuture<StoredSchema>>> storedSchemaCompletableFuture =
                this.schemaStorage.getAll(schemaId);
        try {
            for (int i = 0; i < storedSchemaCompletableFuture.get().size(); i++) {
                CompletableFuture<SchemaAndMetadata> res = new CompletableFuture<>();
                SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.parseFrom(
                        storedSchemaCompletableFuture.get().get(i).get().data);
                SchemaData schemaData = SchemaData.builder()
                        .user(info.getUser())
                        .type(convertToDomainType(info.getType()))
                        .data(info.getSchema().toByteArray())
                        .isDeleted(info.getDeleted())
                        .props(toMap(info.getPropsList()))
                        .build();
                res.complete(new SchemaAndMetadata(schemaId, schemaData,
                        storedSchemaCompletableFuture.get().get(i).get().version));
                result.add(res);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        finalResult.complete(result);
        return finalResult;
    }

    @Override
    public CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId, SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy) {

        return this.schemaStorage.put(schemaId, schema.getData(), hash(schema.getData()));
    }

    @Override
    public CompletableFuture<SchemaVersion> putEmptySchema(String schemaId, String user, boolean force) {
        byte[] schema = new byte[0];
        return this.schemaStorage.put(schemaId, schema, hash(schema));
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaFromStorage(String schemaId) {
        return this.schemaStorage.delete(schemaId);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaFromStorage(String schemaId, boolean forcefully) {
        return this.schemaStorage.delete(schemaId, forcefully);
    }

    @Override
    public CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId) {

        try {
            CompletableFuture<List<SchemaAndMetadata>> finalResult = new CompletableFuture<>();
            List<SchemaAndMetadata> result = new ArrayList<>();
            List<CompletableFuture<SchemaAndMetadata>> allSchemas = getAllSchemas(schemaId).get();

            for (int i = 0; i < allSchemas.size(); i++) {
                result.add(allSchemas.get(i).get());
            }
            finalResult.complete(result);
            return finalResult;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public CompletableFuture<Long> findSchemaVersion(String schemaId, SchemaData schemaData) {
        return trimDeletedSchemaAndGetList(schemaId)
                .thenCompose(schemaAndMetadataList -> {
                    SchemaHash newHash = SchemaHash.of(schemaData);
                    for (SchemaAndMetadata schemaAndMetadata : schemaAndMetadataList) {
                        if (newHash.equals(SchemaHash.of(schemaAndMetadata.schema))) {
                            return completedFuture(((LongSchemaVersion) schemaStorage
                                    .versionFromBytes(schemaAndMetadata.version.bytes())).getVersion());
                        }
                    }
                    return completedFuture(NO_SCHEMA_VERSION);
                });
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        return this.schemaStorage.versionFromBytes(version);
    }

    @Override
    public void initialize(ServiceConfiguration configuration, SchemaStorage schemaStorage)
            throws PulsarServerException {
        this.schemaStorage = schemaStorage;
        this.serviceConfiguration = serviceConfiguration;
    }

    @Override
    public void close() throws Exception {
        // nothing
    }

    private byte[] hash(byte[] val) {
        return val;
    }
}
