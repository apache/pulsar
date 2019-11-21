/**
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
package org.apache.pulsar.tests.integration.utils;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.docker.ContainerExecResultBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DockerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);

    private static File getTargetDirectory(String containerId) {
        String base = System.getProperty("maven.buildDirectory");
        if (base == null) {
            base = "target";
        }
        File directory = new File(base + "/container-logs/" + containerId);
        if (!directory.exists() && !directory.mkdirs()) {
            LOG.error("Error creating directory for container logs.");
        }
        return directory;
    }

    public static void dumpContainerLogToTarget(DockerClient dockerClient, String containerId) {
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        final String containerName = inspectContainerResponse.getName().replace("/","");
        File output = new File(getTargetDirectory(containerName), "docker.log");
        int i = 0;
        while (output.exists()) {
            LOG.info("{} exists, incrementing", output);
            output = new File(getTargetDirectory(containerName), "docker." + i++ + ".log");
        }
        try (FileOutputStream os = new FileOutputStream(output)) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            dockerClient.logContainerCmd(containerName).withStdOut(true)
                .withStdErr(true).withTimestamps(true).exec(new ResultCallback<Frame>() {
                        @Override
                        public void close() {}

                        @Override
                        public void onStart(Closeable closeable) {}

                        @Override
                        public void onNext(Frame object) {
                            try {
                                os.write(object.getPayload());
                            } catch (IOException e) {
                                onError(e);
                            }
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }

                        @Override
                        public void onComplete() {
                            future.complete(true);
                        }
                    });
            future.get();
        } catch (RuntimeException|ExecutionException|IOException e) {
            LOG.error("Error dumping log for {}", containerName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted dumping log from container {}", containerName, ie);
        }
    }

    public static void dumpContainerDirToTargetCompressed(DockerClient dockerClient, String containerId,
                                                          String path) {
        final int READ_BLOCK_SIZE = 10000;
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        final String containerName = inspectContainerResponse.getName().replace("/","");
        final String baseName = path.replace("/", "-").replaceAll("^-", "");
        File output = new File(getTargetDirectory(containerName), baseName + ".tar.gz");
        int i = 0;
        while (output.exists()) {
            LOG.info("{} exists, incrementing", output);
            output = new File(getTargetDirectory(containerName), baseName + "_" + i++ + ".tar.gz");
        }
        try (InputStream dockerStream = dockerClient.copyArchiveFromContainerCmd(containerId, path).exec();
             OutputStream os = new GZIPOutputStream(new FileOutputStream(output))) {
            byte[] block = new byte[READ_BLOCK_SIZE];
            int read = dockerStream.read(block, 0, READ_BLOCK_SIZE);
            while (read > -1) {
                os.write(block, 0, read);
                read = dockerStream.read(block, 0, READ_BLOCK_SIZE);
            }
        } catch (RuntimeException|IOException e) {
            if (!(e instanceof NotFoundException)) {
                LOG.error("Error reading dir from container {}", containerName, e);
            }
        }
    }

    public static void dumpContainerLogDirToTarget(DockerClient docker, String containerId,
                                                   String path) {
        final int READ_BLOCK_SIZE = 10000;

        try (InputStream dockerStream = docker.copyArchiveFromContainerCmd(containerId, path).exec();
             TarArchiveInputStream stream = new TarArchiveInputStream(dockerStream)) {
            TarArchiveEntry entry = stream.getNextTarEntry();
            while (entry != null) {
                if (entry.isFile()) {
                    File output = new File(getTargetDirectory(containerId), entry.getName().replace("/", "-"));
                    try (FileOutputStream os = new FileOutputStream(output)) {
                        byte[] block = new byte[READ_BLOCK_SIZE];
                        int read = stream.read(block, 0, READ_BLOCK_SIZE);
                        while (read > -1) {
                            os.write(block, 0, read);
                            read = stream.read(block, 0, READ_BLOCK_SIZE);
                        }
                    }
                }
                entry = stream.getNextTarEntry();
            }
        } catch (RuntimeException|IOException e) {
            LOG.error("Error reading logs from container {}", containerId, e);
        }
    }

    public static String getContainerIP(DockerClient docker, String containerId) {
        for (Map.Entry<String, ContainerNetwork> e : docker.inspectContainerCmd(containerId)
                 .exec().getNetworkSettings().getNetworks().entrySet()) {
            return e.getValue().getIpAddress();
        }
        throw new IllegalArgumentException("Container " + containerId + " has no networks");
    }

    public static ContainerExecResult runCommand(DockerClient docker,
                                                 String containerId,
                                                 String... cmd)
            throws ContainerExecException, ExecutionException, InterruptedException {
        try {
            return runCommandAsync(docker, containerId, cmd).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ContainerExecException) {
                throw (ContainerExecException) e.getCause();
            }
            throw e;
        }
    }

    public static CompletableFuture<ContainerExecResult> runCommandAsync(DockerClient dockerClient,
                                                                         String containerId,
                                                                         String... cmd) {
        CompletableFuture<ContainerExecResult> future = new CompletableFuture<>();
        String execId = dockerClient.execCreateCmd(containerId)
            .withCmd(cmd)
            .withAttachStderr(true)
            .withAttachStdout(true)
            .exec()
            .getId();
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        final String containerName = inspectContainerResponse.getName().replace("/","");
        String cmdString = String.join(" ", cmd);
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
        dockerClient.execStartCmd(execId).withDetach(false)
            .exec(new ResultCallback<Frame>() {
                @Override
                public void close() {}

                @Override
                public void onStart(Closeable closeable) {
                    LOG.info("DOCKER.exec({}:{}): Executing...", containerName, cmdString);
                }

                @Override
                public void onNext(Frame object) {
                    LOG.info("DOCKER.exec({}:{}): {}", containerName, cmdString, object);
                    if (StreamType.STDOUT == object.getStreamType()) {
                        stdout.append(new String(object.getPayload(), UTF_8));
                    } else if (StreamType.STDERR == object.getStreamType()) {
                        stderr.append(new String(object.getPayload(), UTF_8));
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    LOG.info("DOCKER.exec({}:{}): Done", containerName, cmdString);

                    InspectExecResponse resp = dockerClient.inspectExecCmd(execId).exec();
                    while (resp.isRunning()) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(ie);
                        }
                        resp = dockerClient.inspectExecCmd(execId).exec();
                    }
                    int retCode = resp.getExitCode();
                    ContainerExecResult result = ContainerExecResult.of(
                            retCode,
                            stdout.toString(),
                            stderr.toString()
                    );
                    LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

                    if (retCode != 0) {
                        LOG.error("DOCKER.exec({}:{}): completed with non zero return code: {}\nstdout: {}\nstderr: {}",
                                containerName, cmdString, result.getExitCode(), result.getStdout(), result.getStderr());
                        future.completeExceptionally(new ContainerExecException(cmdString, containerId, result));
                    } else {
                        future.complete(result);
                    }
                }
            });
        return future;
    }

    public static ContainerExecResultBytes runCommandWithRawOutput(DockerClient dockerClient,
                                                                   String containerId,
                                                                   String... cmd) throws ContainerExecException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        String execId = dockerClient.execCreateCmd(containerId)
                .withCmd(cmd)
                .withAttachStderr(true)
                .withAttachStdout(true)
                .exec()
                .getId();
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        final String containerName = inspectContainerResponse.getName().replace("/","");
        String cmdString = String.join(" ", cmd);
        ByteBuf stdout = Unpooled.buffer();
        ByteBuf stderr = Unpooled.buffer();
        dockerClient.execStartCmd(execId).withDetach(false)
                .exec(new ResultCallback<Frame>() {
                    @Override
                    public void close() {
                    }

                    @Override
                    public void onStart(Closeable closeable) {
                        LOG.info("DOCKER.exec({}:{}): Executing...", containerName, cmdString);
                    }

                    @Override
                    public void onNext(Frame object) {
                        if (StreamType.STDOUT == object.getStreamType()) {
                            stdout.writeBytes(object.getPayload());
                        } else if (StreamType.STDERR == object.getStreamType()) {
                            stderr.writeBytes(object.getPayload());
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        LOG.info("DOCKER.exec({}:{}): Done", containerName, cmdString);
                        future.complete(true);
                    }
                });
        future.join();

        InspectExecResponse resp = dockerClient.inspectExecCmd(execId).exec();
        while (resp.isRunning()) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
            resp = dockerClient.inspectExecCmd(execId).exec();
        }
        int retCode = resp.getExitCode();

        byte[] stdoutBytes = new byte[stdout.readableBytes()];
        stdout.readBytes(stdoutBytes);
        byte[] stderrBytes = new byte[stderr.readableBytes()];
        stderr.readBytes(stderrBytes);

        ContainerExecResultBytes result = ContainerExecResultBytes.of(
                retCode,
                stdoutBytes,
                stderrBytes);
        LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

        if (retCode != 0) {
            throw new ContainerExecException(cmdString, containerId, null);
        }
        return result;
    }

    public static Optional<String> getContainerCluster(DockerClient docker, String containerId) {
        return Optional.ofNullable(docker.inspectContainerCmd(containerId)
                                   .exec().getConfig().getLabels().get("cluster"));
    }
}
