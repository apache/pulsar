package org.apache.pulsar.common.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class CompletableFutures {
    public static <T> @Nonnull CompletionStage<T> apply(@Nonnull Supplier<T> fn) {
        try {
            final CompletableFuture<T> future = new CompletableFuture<>();
            if (fn == null) {
                future.completeExceptionally(new NullPointerException("Parameter can not be null."));
                return future;
            }
            future.complete(fn.get());
            return future;
        } catch (Throwable ex) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
    }

    public static <T> @Nonnull CompletionStage<T> compose(@Nonnull Supplier<CompletionStage<T>> fn) {
        try {
            if (fn == null) {
                final CompletableFuture<T> future = new CompletableFuture<>();
                future.completeExceptionally(new NullPointerException("Parameter can not be null."));
                return future;
            }
            return fn.get();
        } catch (Throwable ex) {
            final CompletableFuture<T> future = new CompletableFuture<>();
            future.completeExceptionally(ex);
            return future;
        }
    }
}
