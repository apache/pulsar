/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.concurrent.DefaultThreadFactory;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.WriterInterceptor;
import jakarta.ws.rs.ext.WriterInterceptorContext;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.server.AsyncContext;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class WebExecutorAsyncResponseTest {

    private static final String WEB_THREAD_PREFIX = "test-web";
    private static final String OTHER_THREAD_PREFIX = "test-other";
    private static final String WRITER_THREAD_HEADER = "X-Writer-Thread";
    private static final String RESPONSE_WRITTEN_PROPERTY = "test.responseWritten";

    private ExecutorService otherExecutor;
    private WebExecutorThreadPool webExecutor;
    private Server server;
    private URI baseUri;
    private HttpClient httpClient;
    private TestResource testResource;

    @BeforeClass
    public void setup() throws Exception {
        otherExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory(OTHER_THREAD_PREFIX));
        webExecutor = new WebExecutorThreadPool(8, WEB_THREAD_PREFIX);
        server = new Server(webExecutor);
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);

        ResourceConfig config = new ResourceConfig();
        testResource = new TestResource(otherExecutor);
        config.register(testResource);
        config.register(WriterThreadInterceptor.class);
        config.register(new WebExecutorAsyncResponseFeature(webExecutor));
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        ServletContextHandler context = new ServletContextHandler();
        context.addServlet(servletHolder, "/*");
        server.setHandler(context);
        server.start();

        baseUri = URI.create("http://localhost:" + connector.getLocalPort());
        httpClient = HttpClient.newHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
        if (server != null) {
            server.stop();
        }
        if (otherExecutor != null) {
            otherExecutor.shutdownNow();
        }
    }

    @Path("/test")
    public static class TestResource {
        private final ExecutorService otherExecutor;
        private volatile CompletableFuture<String> completionCallbackResult = new CompletableFuture<>();

        public TestResource(ExecutorService otherExecutor) {
            this.otherExecutor = otherExecutor;
        }

        @GET
        @Path("/resumeFromOtherThread")
        public void resumeFromOtherThread(@Suspended AsyncResponse asyncResponse) {
            otherExecutor.execute(() -> asyncResponse.resume("resumed"));
        }

        @GET
        @Path("/resumeExceptionFromOtherThread")
        public void resumeExceptionFromOtherThread(@Suspended AsyncResponse asyncResponse) {
            otherExecutor.execute(() -> asyncResponse.resume(new WebApplicationException(
                    Response.status(Response.Status.CONFLICT).entity("failed").build())));
        }

        @GET
        @Path("/resumeWithCompletionCallback")
        public void resumeWithCompletionCallback(@Suspended AsyncResponse asyncResponse,
                                                 @Context ContainerRequestContext requestContext) {
            CompletableFuture<String> result = completionCallbackResult;
            // the interceptor sets it once it has written the response
            AtomicBoolean responseWritten = new AtomicBoolean();
            requestContext.setProperty(RESPONSE_WRITTEN_PROPERTY, responseWritten);
            asyncResponse.register((CompletionCallback) throwable -> result.complete(
                    "written=" + responseWritten.get() + ", thread=" + Thread.currentThread().getName()));
            otherExecutor.execute(() -> asyncResponse.resume("resumed"));
        }

        @GET
        @Path("/resumeAsyncContextFromOtherThread")
        public void resumeAsyncContextFromOtherThread(@Suspended AsyncContext asyncContext) {
            otherExecutor.execute(() -> asyncContext.resume("resumed"));
        }

        @GET
        @Path("/resumeOnRequestThread")
        public void resumeOnRequestThread(@Suspended AsyncResponse asyncResponse) {
            asyncResponse.resume("resumed");
        }
    }

    public static class WriterThreadInterceptor implements WriterInterceptor {
        @Override
        public void aroundWriteTo(WriterInterceptorContext context) throws IOException {
            context.getHeaders().add(WRITER_THREAD_HEADER, Thread.currentThread().getName());
            context.proceed();
            if (context.getProperty(RESPONSE_WRITTEN_PROPERTY) instanceof AtomicBoolean responseWritten) {
                responseWritten.set(true);
            }
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        return httpClient.send(HttpRequest.newBuilder(baseUri.resolve(path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    public void testResumeFromOtherThreadWritesOnWebExecutor() throws Exception {
        HttpResponse<String> response = get("/test/resumeFromOtherThread");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("resumed");
        assertThat(response.headers().firstValue(WRITER_THREAD_HEADER)).hasValueSatisfying(
                threadName -> assertThat(threadName).startsWith(WEB_THREAD_PREFIX));
    }

    @Test
    public void testResumeExceptionFromOtherThreadWritesOnWebExecutor() throws Exception {
        HttpResponse<String> response = get("/test/resumeExceptionFromOtherThread");
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).isEqualTo("failed");
        assertThat(response.headers().firstValue(WRITER_THREAD_HEADER)).hasValueSatisfying(
                threadName -> assertThat(threadName).startsWith(WEB_THREAD_PREFIX));
    }

    @Test
    public void testCompletionCallbackRunsAfterResponseIsWritten() throws Exception {
        // callbacks such as the topic list permit release in NamespacesBase rely on running after the write
        CompletableFuture<String> callbackResult = new CompletableFuture<>();
        testResource.completionCallbackResult = callbackResult;
        HttpResponse<String> response = get("/test/resumeWithCompletionCallback");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("resumed");
        assertThat(callbackResult.get(10, TimeUnit.SECONDS))
                .startsWith("written=true, thread=" + WEB_THREAD_PREFIX);
    }

    @Test
    public void testAsyncContextParameterIsLeftToJersey() throws Exception {
        // the wrapper doesn't implement Jersey's AsyncContext, so such a parameter gets Jersey's own object
        HttpResponse<String> response = get("/test/resumeAsyncContextFromOtherThread");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("resumed");
    }

    @Test
    public void testResumeOnRequestThread() throws Exception {
        HttpResponse<String> response = get("/test/resumeOnRequestThread");
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("resumed");
        assertThat(response.headers().firstValue(WRITER_THREAD_HEADER)).hasValueSatisfying(
                threadName -> assertThat(threadName).startsWith(WEB_THREAD_PREFIX));
    }

    @Test
    public void testResumeOnPoolThreadRunsInline() throws Exception {
        AsyncResponse delegate = mock(AsyncResponse.class);
        when(delegate.resume(any(Object.class))).thenReturn(true);
        WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, webExecutor);
        AtomicReference<Boolean> result = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        webExecutor.execute(() -> {
            result.set(asyncResponse.resume("value"));
            // resumed before returning, on this thread
            verify(delegate).resume("value");
            done.countDown();
        });
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).isTrue();
    }

    @Test
    public void testCompletionIsDispatchedOnceWhileQueued() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, "test-blocked");
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockPool(pool, 2, release);
            AsyncResponse delegate = mock(AsyncResponse.class);
            when(delegate.isSuspended()).thenReturn(true);
            CompletableFuture<String> resumingThread = new CompletableFuture<>();
            when(delegate.resume(any(Object.class))).thenAnswer(invocation -> {
                resumingThread.complete(Thread.currentThread().getName());
                return true;
            });
            WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, pool);

            assertThat(asyncResponse.resume("first")).isTrue();
            // Jersey's state is reported until the queued completion has run
            assertThat(asyncResponse.isDone()).isFalse();
            assertThat(asyncResponse.isSuspended()).isTrue();
            assertThat(asyncResponse.resume("second")).isFalse();
            assertThat(asyncResponse.resume(new RuntimeException("third"))).isFalse();
            assertThat(asyncResponse.cancel()).isFalse();
            assertThat(asyncResponse.setTimeout(1, TimeUnit.SECONDS)).isFalse();
            verify(delegate, never()).resume(any(Object.class));

            release.countDown();
            // Mockito records the invocation before it runs the answer, so wait for the answer
            assertThat(resumingThread.get(10, TimeUnit.SECONDS)).startsWith("test-blocked");
            verify(delegate).resume("first");
            verify(delegate, never()).resume("second");
            verify(delegate, never()).resume(any(Throwable.class));
            verify(delegate, never()).cancel();
            verify(delegate, never()).setTimeout(anyLong(), any());
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test
    public void testCancellingAgainSucceeds() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, "test-blocked");
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockPool(pool, 2, release);
            AsyncResponse delegate = mock(AsyncResponse.class);
            when(delegate.cancel()).thenReturn(true);
            WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, pool);

            assertThat(asyncResponse.cancel()).isTrue();
            // cancelling again succeeds while the cancellation is queued
            assertThat(asyncResponse.cancel()).isTrue();
            assertThat(asyncResponse.cancel(10)).isTrue();
            assertThat(asyncResponse.resume("value")).isFalse();

            release.countDown();
            verify(delegate, timeout(10000)).cancel();
            when(delegate.isDone()).thenReturn(true);
            when(delegate.isCancelled()).thenReturn(true);
            // and after it has run
            assertThat(asyncResponse.cancel()).isTrue();
            verify(delegate).cancel();
            verify(delegate, never()).cancel(anyInt());
            verify(delegate, never()).resume(any(Object.class));
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test
    public void testCancellingAgainSucceedsWhileQueuedCancellationCompletes() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, "test-blocked");
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockPool(pool, 2, release);
            AsyncResponse delegate = mock(AsyncResponse.class);
            AtomicBoolean cancelled = new AtomicBoolean();
            when(delegate.isDone()).thenAnswer(invocation -> cancelled.get());
            // the queued cancellation completes right after the first check of Jersey's state
            when(delegate.isCancelled()).thenAnswer(invocation -> cancelled.getAndSet(true));
            WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, pool);

            assertThat(asyncResponse.cancel()).isTrue();
            assertThat(asyncResponse.cancel()).isTrue();
            assertThat(asyncResponse.cancel()).isTrue();
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test
    public void testCompletionRunsOnCallingThreadWhenTimeoutIsSet() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, "test-blocked");
        pool.start();
        CountDownLatch release = new CountDownLatch(1);
        try {
            blockPool(pool, 2, release);
            AsyncResponse delegate = mock(AsyncResponse.class);
            when(delegate.setTimeout(anyLong(), any())).thenReturn(true);
            AtomicReference<Thread> resumingThread = new AtomicReference<>();
            when(delegate.resume(any(Object.class))).thenAnswer(invocation -> {
                resumingThread.set(Thread.currentThread());
                return true;
            });
            WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, pool);

            assertThat(asyncResponse.setTimeout(1, TimeUnit.SECONDS)).isTrue();
            // a Jersey timeout can't overtake a queued completion, since the completion isn't queued
            assertThat(asyncResponse.resume("value")).isTrue();
            verify(delegate).resume("value");
            assertThat(resumingThread.get()).isSameAs(Thread.currentThread());
        } finally {
            release.countDown();
            pool.stop();
        }
    }

    @Test
    public void testRejectedCompletionRunsOnCallingThread() throws Exception {
        WebExecutorThreadPool pool = new WebExecutorThreadPool(2, "test-stopped");
        pool.start();
        pool.stop();
        AsyncResponse delegate = mock(AsyncResponse.class);
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        when(delegate.resume(any(Object.class))).thenAnswer(invocation -> {
            resumingThread.set(Thread.currentThread());
            return true;
        });
        WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, pool);

        assertThat(asyncResponse.resume("value")).isTrue();
        verify(delegate).resume("value");
        assertThat(resumingThread.get()).isSameAs(Thread.currentThread());
    }

    @Test
    public void testCompletedDelegateIsCompletedOnCallingThread() {
        AsyncResponse delegate = mock(AsyncResponse.class);
        when(delegate.isDone()).thenReturn(true);
        AtomicReference<Thread> resumingThread = new AtomicReference<>();
        when(delegate.resume(any(Object.class))).thenAnswer(invocation -> {
            resumingThread.set(Thread.currentThread());
            return false;
        });
        WebExecutorAsyncResponse asyncResponse = new WebExecutorAsyncResponse(delegate, webExecutor);

        // Jersey's response decides the result, and doesn't write anything since it's already done
        assertThat(asyncResponse.resume("value")).isFalse();
        verify(delegate).resume("value");
        assertThat(resumingThread.get()).isSameAs(Thread.currentThread());
    }

    // occupies all the pool's threads so that completions stay queued until release is counted down
    private static void blockPool(WebExecutorThreadPool pool, int poolSize, CountDownLatch release)
            throws InterruptedException {
        CountDownLatch poolBlocked = new CountDownLatch(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.execute(() -> {
                poolBlocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertThat(poolBlocked.await(10, TimeUnit.SECONDS)).isTrue();
    }
}
