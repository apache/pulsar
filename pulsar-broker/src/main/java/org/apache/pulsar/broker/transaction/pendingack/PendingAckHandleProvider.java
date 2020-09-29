package org.apache.pulsar.broker.transaction.pendingack;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;

public interface PendingAckHandleProvider {

    /**
     * Construct a provider from the provided class.
     *
     * @param providerClassName the provider class name.
     * @return an instance of transaction buffer provider.
     */
    static PendingAckHandleProvider newProvider(String providerClassName) throws IOException {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(providerClassName);
            Object obj = providerClass.newInstance();
            checkArgument(obj instanceof PendingAckHandleProvider,
                    "The factory has to be an instance of "
                            + PendingAckHandleProvider.class.getName());

            return (PendingAckHandleProvider) obj;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Open the pending ack handle.
     *
     * @param cursor {@link ManagedCursor}
     * @param topicName {@link String}
     * @param subName {@link String}
     * @return a future represents the result of the operation.
     *         an instance of {@link PendingAckHandle} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<PendingAckHandle> newPendingAckHandle(ManagedCursor cursor, String topicName, String subName);

}
