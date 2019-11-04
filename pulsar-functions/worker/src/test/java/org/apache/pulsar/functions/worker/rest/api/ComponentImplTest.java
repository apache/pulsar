package org.apache.pulsar.functions.worker.rest.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.bookkeeper.api.kv.result.DeleteResult;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.WorkerUtils;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.FUNCTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;

@PrepareForTest({WorkerUtils.class, InstanceUtils.class})
@PowerMockIgnore({ "javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*" })
public class ComponentImplTest {
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";

    @Test
    public void testDeleteState() throws Exception {
        String key = "some_key";
        AuthenticationDataSource authenticationDataSource = mock(AuthenticationDataSource.class);

        WorkerService worker = mock(WorkerService.class);
        StorageAdminClient adminClient = mock(StorageAdminClient.class);
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        ComponentImpl component = mock(ComponentImpl.class);

        when(component.deleteFunctionState(any(), any(), any(), any(), any(), any())).thenCallRealMethod();

        Whitebox.setInternalState(component, "componentType", FUNCTION);

        when(component.isWorkerServiceAvailable()).thenReturn(true);
        when(component.worker()).thenReturn(worker);
        when(component.isAuthorizedRole(any(), any(), any(), any())).thenReturn(true);
        when(worker.getStateStoreAdminClient()).thenReturn(adminClient);
        when(worker.getWorkerConfig()).thenReturn(workerConfig);
        when(workerConfig.getStateStorageServiceUrl()).thenReturn("some_state_storage_url");

        StorageClient storageClient = mock(StorageClient.class);
        AtomicReference<StorageClient> storageClientReference = new AtomicReference<>(storageClient);
        Whitebox.setInternalState(component, "storageClient", storageClientReference);
        Table<ByteBuf, ByteBuf> table = mock(Table.class);
        when(storageClient.openTable(any()))
                .thenReturn(CompletableFuture.completedFuture(table));

        DeleteResult<ByteBuf, ByteBuf> deleteResult = mock(DeleteResult.class);
        when(table.delete(eq(Unpooled.wrappedBuffer(key.getBytes(UTF_8))), eq(Options.delete())))
                .thenReturn(CompletableFuture.completedFuture(deleteResult));

        assertTrue(component.deleteFunctionState(tenant, namespace, function, key, null, authenticationDataSource));
    }
}