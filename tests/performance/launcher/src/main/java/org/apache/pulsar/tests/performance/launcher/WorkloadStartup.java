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
package org.apache.pulsar.tests.performance.launcher;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

/**
 * Waits until a workload container logs its ready line, and shows the startup progress that it logs meanwhile, such as
 * how many of the applications' pods are open. Unlike a wait for a log message, it fails as soon as the container
 * exits, with the cause from its log, and when the container hasn't made progress for {@link #STALL_TIMEOUT}: each
 * new progress line starts the time over, so that a large workload may take as long as it needs while it advances.
 *
 * <p>It keeps the container's output, which the launcher saves when the startup fails: Testcontainers removes a
 * container whose startup failed, and its log with it.
 */
final class WorkloadStartup extends AbstractWaitStrategy {
    /** Starts a line of a workload's startup progress, as the workloads print it. */
    static final String PROGRESS_PREFIX = "PROGRESS ";
    static final Duration STALL_TIMEOUT = Duration.ofSeconds(60);
    private static final long POLL_MILLIS = 1000;

    private final Pattern ready;
    private final Consumer<String> progress;
    private final StringBuilder output = new StringBuilder();
    private String lastProgress;
    private volatile String failure;

    /**
     * @param ready the ready line's regular expression
     * @param progress shows a progress line, without its prefix
     */
    WorkloadStartup(String ready, Consumer<String> progress) {
        this.ready = Pattern.compile(ready);
        this.progress = progress;
    }

    @Override
    protected void waitUntilReady() {
        BlockingQueue<String> frames = new LinkedBlockingQueue<>();
        try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
            Consumer<OutputFrame> collector = frame -> {
                String text = frame.getUtf8String();
                if (text != null && !text.isEmpty()) {
                    frames.add(text);
                }
            };
            callback.addConsumer(OutputFrame.OutputType.STDOUT, collector);
            callback.addConsumer(OutputFrame.OutputType.STDERR, collector);
            waitStrategyTarget.getDockerClient().logContainerCmd(waitStrategyTarget.getContainerId())
                    .withFollowStream(true).withSince(0).withStdOut(true).withStdErr(true).exec(callback);
            awaitReady(() -> frames.poll(POLL_MILLIS, TimeUnit.MILLISECONDS), waitStrategyTarget::isRunning,
                    System::nanoTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw fail("was interrupted while starting");
        } catch (IOException e) {
            throw new ContainerLaunchException("Couldn't follow the container's log", e);
        }
    }

    /** The next frame of the container's output, or null when none came within a poll. */
    interface Frames {
        String poll() throws InterruptedException;
    }

    /**
     * Waits for the ready line: fails when the container exits, and when it hasn't made progress for
     * {@link #STALL_TIMEOUT}, also while it keeps writing other output, such as the warnings of retries.
     */
    void awaitReady(Frames frames, BooleanSupplier running, LongSupplier nanoTime) throws InterruptedException {
        long stalledAt = nanoTime.getAsLong() + STALL_TIMEOUT.toNanos();
        while (true) {
            String frame = frames.poll();
            if (frame != null) {
                Line line = accept(frame);
                if (line == Line.READY) {
                    return;
                }
                if (line == Line.PROGRESS) {
                    stalledAt = nanoTime.getAsLong() + STALL_TIMEOUT.toNanos();
                }
            } else if (!running.getAsBoolean()) {
                // The output that the container wrote before it exited may still be on its way
                for (String rest; (rest = frames.poll()) != null; ) {
                    if (accept(rest) == Line.READY) {
                        return;
                    }
                }
                throw fail("exited with status " + exitCode() + causeSuffix());
            }
            if (nanoTime.getAsLong() > stalledAt) {
                throw fail("made no progress for " + STALL_TIMEOUT.toSeconds() + " s" + causeSuffix());
            }
        }
    }

    /** What a frame of the container's output is: its ready line, a new progress line, or other output. */
    enum Line { READY, PROGRESS, OTHER }

    /** Keeps a frame of the container's output, and shows its new progress lines. */
    Line accept(String frame) {
        Line result = Line.OTHER;
        synchronized (output) {
            output.append(frame);
        }
        for (String line : frame.split("\\R")) {
            if (ready.matcher(line).matches()) {
                return Line.READY;
            }
            if (line.startsWith(PROGRESS_PREFIX)) {
                String message = line.substring(PROGRESS_PREFIX.length()).strip();
                if (!message.equals(lastProgress)) {
                    lastProgress = message;
                    progress.accept(message);
                    result = Line.PROGRESS;
                }
            }
        }
        return result;
    }

    /** The container's output so far. */
    String output() {
        synchronized (output) {
            return output.toString();
        }
    }

    /** Why the startup failed, such as "exited with status 3: OutOfMemoryError: Java heap space", or null. */
    String failure() {
        return failure;
    }

    private ContainerLaunchException fail(String reason) {
        failure = reason;
        return new ContainerLaunchException("The container " + reason);
    }

    private String causeSuffix() {
        String cause = FailureCause.of(output());
        return cause != null ? ": " + cause : "";
    }

    private Object exitCode() {
        try {
            return waitStrategyTarget.getDockerClient().inspectContainerCmd(waitStrategyTarget.getContainerId())
                    .exec().getState().getExitCodeLong();
        } catch (RuntimeException e) {
            return "unknown";
        }
    }
}
