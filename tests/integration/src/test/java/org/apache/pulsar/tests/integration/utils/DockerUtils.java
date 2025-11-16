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
package org.apache.pulsar.tests.integration.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectExecResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.docker.ContainerExecResultBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DockerUtils.class);
    
    // Default timeout for Docker command execution
    private static final long DEFAULT_DOCKER_COMMAND_TIMEOUT_SECONDS = 60;
    
    // Diagnostic collection timeout
    private static final long DIAGNOSTIC_COLLECTION_TIMEOUT_SECONDS = 30;

    // Metrics collection for observability enhancement
    private static final AtomicInteger totalDockerCommands = new AtomicInteger(0);
    private static final AtomicInteger timedOutDockerCommands = new AtomicInteger(0);
    private static final AtomicLong totalDockerCommandExecutionTime = new AtomicLong(0);
    private static final AtomicInteger totalDiagnosticCollections = new AtomicInteger(0);
    private static final AtomicInteger failedDiagnosticCollections = new AtomicInteger(0);

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
        final String containerName = getContainerName(dockerClient, containerId);
        File output = getUniqueFileInTargetDirectory(containerName, "docker", ".log");
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(output))) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            dockerClient.logContainerCmd(containerName).withStdOut(true)
                    .withStdErr(true).withTimestamps(true).exec(new ResultCallback<Frame>() {
                @Override
                public void close() {
                }

                @Override
                public void onStart(Closeable closeable) {
                }

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
            future.get(DIAGNOSTIC_COLLECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (RuntimeException | ExecutionException | IOException | TimeoutException e) {
            LOG.error("Error dumping log for {}", containerName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted dumping log from container {}", containerName, ie);
        }
    }

    private static File getUniqueFileInTargetDirectory(String containerName, String prefix, String suffix) {
        return getUniqueFileInDirectory(getTargetDirectory(containerName), prefix, suffix);
    }

    private static File getUniqueFileInDirectory(File directory, String prefix, String suffix) {
        File file = new File(directory, prefix + suffix);
        int i = 0;
        while (file.exists()) {
            LOG.info("{} exists, incrementing", file);
            file = new File(directory, prefix + "_" + (i++) + suffix);
        }
        return file;
    }

    private static String getContainerName(DockerClient dockerClient, String containerId) {
        final InspectContainerResponse inspectContainerResponse = dockerClient.inspectContainerCmd(containerId).exec();
        // docker api returns names prefixed with "/", it's part of it's legacy design,
        // this removes it to be consistent with what docker ps shows.
        return inspectContainerResponse.getName().replace("/", "");
    }

    public static void dumpContainerDirToTargetCompressed(DockerClient dockerClient, String containerId,
                                                          String path) {
        final String containerName = getContainerName(dockerClient, containerId);
        final String baseName = path.replace("/", "-").replaceAll("^-", "");
        File output = getUniqueFileInTargetDirectory(containerName, baseName, ".tar.gz");
        try (InputStream dockerStream = dockerClient.copyArchiveFromContainerCmd(containerId, path).exec();
             OutputStream os = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(output)))) {
            IOUtils.copy(dockerStream, os);
        } catch (RuntimeException | IOException e) {
            if (!(e instanceof NotFoundException)) {
                LOG.error("Error reading dir from container {}", containerName, e);
            }
        }
    }

    public static void dumpContainerLogDirToTarget(DockerClient docker, String containerId,
                                                   String path) {
        File targetDirectory = getTargetDirectory(containerId);
        try (InputStream dockerStream = docker.copyArchiveFromContainerCmd(containerId, path).exec();
             TarArchiveInputStream stream = new TarArchiveInputStream(dockerStream)) {
            TarArchiveEntry entry = stream.getNextTarEntry();
            while (entry != null) {
                if (entry.isFile()) {
                    File output = new File(targetDirectory, entry.getName().replace("/", "-"));
                    Files.copy(stream, output.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
                entry = stream.getNextTarEntry();
            }
        } catch (RuntimeException | IOException e) {
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
        long startTime = System.currentTimeMillis();
        boolean timedOut = false;
        totalDockerCommands.incrementAndGet();
        
        try {
            // Add timeout for Docker command execution to prevent infinite waiting
            ContainerExecResult result = runCommandAsync(docker, containerId, cmd)
                .get(DEFAULT_DOCKER_COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return result;
        } catch (TimeoutException e) {
            timedOut = true;
            timedOutDockerCommands.incrementAndGet();
            // Collect diagnostics when timeout occurs
            collectDiagnostics(docker, containerId);
            throw new ContainerExecException(
                "Command execution timed out after " + DEFAULT_DOCKER_COMMAND_TIMEOUT_SECONDS + " seconds: " 
                + String.join(" ", cmd),
                containerId,
                null);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ContainerExecException) {
                throw (ContainerExecException) e.getCause();
            }
            throw e;
        } finally {
            long executionTime = System.currentTimeMillis() - startTime;
            totalDockerCommandExecutionTime.addAndGet(executionTime);
            LOG.debug("Docker command completed in {} ms, timed out: {}", executionTime, timedOut);
        }
    }

    public static ContainerExecResult runCommandAsUser(String userId,
                                                       DockerClient docker,
                                                       String containerId,
                                                       String... cmd)
            throws ContainerExecException, ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        boolean timedOut = false;
        totalDockerCommands.incrementAndGet();
        
        try {
            // Add timeout for Docker command execution to prevent infinite waiting
            ContainerExecResult result = runCommandAsyncAsUser(userId, docker, containerId, cmd)
                .get(DEFAULT_DOCKER_COMMAND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return result;
        } catch (TimeoutException e) {
            timedOut = true;
            timedOutDockerCommands.incrementAndGet();
            // Collect diagnostics when timeout occurs
            collectDiagnostics(docker, containerId);
            throw new ContainerExecException(
                "Command execution timed out after " + DEFAULT_DOCKER_COMMAND_TIMEOUT_SECONDS + " seconds: " 
                + String.join(" ", cmd),
                containerId,
                null);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ContainerExecException) {
                throw (ContainerExecException) e.getCause();
            }
            throw e;
        } finally {
            long executionTime = System.currentTimeMillis() - startTime;
            totalDockerCommandExecutionTime.addAndGet(executionTime);
            LOG.debug("Docker command (as user) completed in {} ms, timed out: {}", executionTime, timedOut);
        }
    }

    public static CompletableFuture<ContainerExecResult> runCommandAsyncAsUser(String userId,
                                                                               DockerClient dockerClient,
                                                                               String containerId,
                                                                               String... cmd) {
        String execId = createExecCreateCmd(dockerClient, containerId, cmd)
                .withUser(userId)
                .exec()
                .getId();
        return runCommandAsync(execId, dockerClient, containerId, cmd);
    }

    public static CompletableFuture<ContainerExecResult> runCommandAsync(DockerClient dockerClient,
                                                                         String containerId,
                                                                         String... cmd) {
        String execId = createExecCreateCmd(dockerClient, containerId, cmd)
                .exec()
                .getId();
        return runCommandAsync(execId, dockerClient, containerId, cmd);
    }

    private static CompletableFuture<ContainerExecResult> runCommandAsync(String execId,
                                                                          DockerClient dockerClient,
                                                                          String containerId,
                                                                          String... cmd) {
        CompletableFuture<ContainerExecResult> future = new CompletableFuture<>();
        final String containerName = getContainerName(dockerClient, containerId);
        String cmdString = String.join(" ", cmd);
        StringBuilder stdout = new StringBuilder();
        StringBuilder stderr = new StringBuilder();
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

                        InspectExecResponse resp = waitForExecCmdToFinish(dockerClient, execId);
                        long retCode = resp.getExitCodeLong();
                        ContainerExecResult result = ContainerExecResult.of(
                                retCode,
                                stdout.toString(),
                                stderr.toString()
                        );
                        LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

                        if (retCode != 0) {
                            LOG.error(
                                    "DOCKER.exec({}:{}): completed with non zero return code: {}\nstdout: {}\nstderr:"
                                            + " {}",
                                    containerName, cmdString, result.getExitCode(), result.getStdout(),
                                    result.getStderr());
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
        String execId = createExecCreateCmd(dockerClient, containerId, cmd)
                .exec()
                .getId();
        final String containerName = getContainerName(dockerClient, containerId);
        String cmdString = String.join(" ", cmd);
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
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
                        try {
                            if (StreamType.STDOUT == object.getStreamType()) {
                                stdout.write(object.getPayload());
                            } else if (StreamType.STDERR == object.getStreamType()) {
                                stderr.write(object.getPayload());
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
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

        InspectExecResponse resp = waitForExecCmdToFinish(dockerClient, execId);
        long retCode = resp.getExitCodeLong();

        ContainerExecResultBytes result = ContainerExecResultBytes.of(
                retCode,
                stdout.toByteArray(),
                stderr.toByteArray());
        LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);

        if (retCode != 0) {
            throw new ContainerExecException(cmdString, containerId, null);
        }
        return result;
    }

    public static CompletableFuture<Long> runCommandAsyncWithLogging(DockerClient dockerClient,
                                                                        String containerId, String... cmd) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        String execId = createExecCreateCmd(dockerClient, containerId, cmd)
                .exec()
                .getId();
        final String containerName = getContainerName(dockerClient, containerId);
        String cmdString = String.join(" ", cmd);
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
                        LOG.info("DOCKER.exec({}:{}): {}", containerName, cmdString, object);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        LOG.info("DOCKER.exec({}:{}): Done", containerName, cmdString);
                        InspectExecResponse resp = waitForExecCmdToFinish(dockerClient, execId);
                        long retCode = resp.getExitCodeLong();
                        LOG.info("DOCKER.exec({}:{}): completed with {}", containerName, cmdString, retCode);
                        future.complete(retCode);
                    }
                });
        return future;
    }

    private static ExecCreateCmd createExecCreateCmd(DockerClient dockerClient, String containerId, String[] cmd) {
        return dockerClient.execCreateCmd(containerId)
                .withCmd(cmd)
                .withAttachStderr(true)
                .withAttachStdout(true)
                // Clear out PULSAR_EXTRA_OPTS and OPTS so that they don't interfere with the test
                .withEnv(List.of("PULSAR_EXTRA_OPTS=", "OPTS="));
    }

    private static InspectExecResponse waitForExecCmdToFinish(DockerClient dockerClient, String execId) {
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
        return resp;
    }

    public static Optional<String> getContainerCluster(DockerClient docker, String containerId) {
        return Optional.ofNullable(docker.inspectContainerCmd(containerId)
                .exec().getConfig().getLabels().get("cluster"));
    }
    
    /**
     * Collect comprehensive diagnostics information when Docker command execution times out.
     * This helps with debugging timeout issues.
     */
    private static void collectDiagnostics(DockerClient docker, String containerId) {
        totalDiagnosticCollections.incrementAndGet();
        try {
            LOG.info("Collecting diagnostics for container {}", containerId);
            
            // Collect container log
            LOG.info("Collecting container log for {}", containerId);
            dumpContainerLogToTarget(docker, containerId);
            
            // Collect container state
            LOG.info("Collecting container state for {}", containerId);
            InspectContainerResponse info = docker.inspectContainerCmd(containerId).exec();
            LOG.error("Container state: {}", info.getState());
            
            // Collect running processes
            LOG.info("Collecting running processes for {}", containerId);
            try {
                com.github.dockerjava.api.command.TopContainerResponse top = docker.topContainerCmd(containerId).exec();
                LOG.error("Running processes: {}", top);
            } catch (Exception e) {
                LOG.error("Failed to collect running processes", e);
            }
            
            // Collect container configuration
            LOG.info("Collecting container configuration for {}", containerId);
            LOG.error("Container config: {}", info.getConfig());
            
            // Collect container network settings
            LOG.info("Collecting container network settings for {}", containerId);
            LOG.error("Container network settings: {}", info.getNetworkSettings());
            
            // Try to collect function logs if this is a worker container
            LOG.info("Collecting function logs for {}", containerId);
            try {
                dumpContainerDirToTargetCompressed(docker, containerId, "/pulsar/logs/functions");
            } catch (Exception e) {
                LOG.warn("Failed to collect function logs", e);
            }
            
            // Try to collect Pulsar logs
            LOG.info("Collecting Pulsar logs for {}", containerId);
            try {
                dumpContainerDirToTargetCompressed(docker, containerId, "/pulsar/logs");
            } catch (Exception e) {
                LOG.warn("Failed to collect Pulsar logs", e);
            }
            
            // Collect function-specific diagnostics for Pulsar Functions containers
            collectFunctionSpecificDiagnostics(docker, containerId);
            
            LOG.info("Finished collecting diagnostics for container {}", containerId);
        } catch (Exception e) {
            failedDiagnosticCollections.incrementAndGet();
            LOG.error("Failed to collect diagnostics for container {}", containerId, e);
        }
    }
    
    /**
     * Collect function-specific diagnostics for Pulsar Functions containers.
     * This includes function metadata, configuration files, and instance information.
     */
    private static void collectFunctionSpecificDiagnostics(DockerClient docker, String containerId) {
        try {
            // Get container name
            String containerName = getContainerName(docker, containerId);
            
            // Collect configuration files
            try {
                LOG.info("Collecting configuration files for {}", containerName);
                dumpContainerDirToTargetCompressed(docker, containerId, "/pulsar/conf");
            } catch (Exception e) {
                LOG.warn("Failed to collect configuration files", e);
            }
            
            // Collect function metadata
            try {
                LOG.info("Collecting function metadata for {}", containerName);
                // Try to get function information
                CompletableFuture<ContainerExecResult> future = runCommandAsync(docker, containerId,
                    "/pulsar/bin/pulsar-admin", "functions", "list");
                
                // Add a reasonable timeout for this diagnostic command
                ContainerExecResult result = future.get(10, TimeUnit.SECONDS);
                LOG.info("Functions in container {}: {}", containerName, result.getStdout());
            } catch (Exception e) {
                LOG.warn("Failed to collect function metadata", e);
            }
            
        } catch (Exception e) {
            LOG.error("Failed to collect function-specific diagnostics", e);
        }
    }
    
    /**
     * Print a summary of Docker command metrics for observability.
     * This helps in identifying performance bottlenecks and timeout issues.
     */
    public static void printDockerMetricsSummary() {
        LOG.info("===== Docker Command Metrics Summary =====");
        LOG.info("Total Docker commands executed: {}", totalDockerCommands.get());
        LOG.info("Docker commands timed out: {}", timedOutDockerCommands.get());
        LOG.info("Diagnostic collections attempted: {}", totalDiagnosticCollections.get());
        LOG.info("Failed diagnostic collections: {}", failedDiagnosticCollections.get());
        
        if (totalDockerCommands.get() > 0) {
            LOG.info("Docker command timeout rate: {}%", 
                (timedOutDockerCommands.get() * 100.0 / totalDockerCommands.get()));
        }
        
        if (totalDockerCommands.get() > 0) {
            LOG.info("Average Docker command execution time: {} ms", 
                totalDockerCommandExecutionTime.get() / totalDockerCommands.get());
        }
        LOG.info("=====================================");
    }
    
    /**
     * Reset all Docker command metrics.
     * This is useful for cleaning up between test runs.
     */
    public static void resetDockerMetrics() {
        totalDockerCommands.set(0);
        timedOutDockerCommands.set(0);
        totalDockerCommandExecutionTime.set(0);
        totalDiagnosticCollections.set(0);
        failedDiagnosticCollections.set(0);
    }
    
    /**
     * Force kill a container if it's not responding.
     * This should be used as a last resort when other methods fail.
     */
    public static void forceKillContainer(DockerClient docker, String containerId) {
        try {
            LOG.warn("Force killing container {}", containerId);
            docker.killContainerCmd(containerId).exec();
        } catch (Exception e) {
            LOG.error("Failed to force kill container {}", containerId, e);
        }
    }
    
    /**
     * Restart a container.
     */
    public static void restartContainer(DockerClient docker, String containerId) {
        try {
            LOG.info("Restarting container {}", containerId);
            docker.restartContainerCmd(containerId).exec();
        } catch (Exception e) {
            LOG.error("Failed to restart container {}", containerId, e);
        }
    }
}