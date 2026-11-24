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
package org.apache.pulsar.tests.integration.nativeimage;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.tests.TestRetrySupport;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Drives the natively-compiled {@link NativeImageTesterApp} binary against a real Pulsar
 * broker. The binary is built by the {@code native-maven-plugin} (see this module's
 * {@code nativeImageTests} profile) using only the GraalVM metadata embedded in
 * {@code pulsar-client-original} and {@code pulsar-client-admin-original}.
 *
 * <p>The fact that the binary builds at all proves the embedded reflection/resource
 * configuration is discovered from the classpath; these tests additionally prove it works at
 * runtime by performing produce/consume and an admin REST call.
 */
public class NativeImageSmokeTest extends TestRetrySupport {

    private static final String SUCCESS_MARKER = "NATIVE_IMAGE_TEST_SUCCESS";
    private static final int PROCESS_TIMEOUT_SECONDS = 120;

    private PulsarContainer pulsarContainer;

    @Override
    @BeforeClass(alwaysRun = true)
    public final void setup() {
        incrementSetupNumber();
        pulsarContainer = new PulsarContainer();
        pulsarContainer.start();
    }

    @Test
    public void nativeImageProduceConsume() throws Exception {
        ProcessResult result = runNativeBinary(
                "produce-consume",
                "--service-url", pulsarContainer.getPlainTextPulsarBrokerUrl(),
                "--topic", "native-image-smoke-test");
        assertSuccess(result);
    }

    @Test
    public void nativeImageAdmin() throws Exception {
        ProcessResult result = runNativeBinary(
                "admin",
                "--admin-url", pulsarContainer.getPulsarAdminUrl());
        assertSuccess(result);
    }

    private void assertSuccess(ProcessResult result) {
        Assert.assertEquals(result.exitCode, 0,
                "Native process exited non-zero.\n" + result);
        Assert.assertTrue(result.stdout.contains(SUCCESS_MARKER),
                "Expected success marker '" + SUCCESS_MARKER + "' not found.\n" + result);
    }

    private ProcessResult runNativeBinary(String... arguments) throws Exception {
        File binary = locateNativeBinary();
        if (!binary.exists()) {
            throw new SkipException("Native binary not found at " + binary.getAbsolutePath()
                    + " — build it with -PnativeImageTests using a GraalVM native-image toolchain.");
        }

        List<String> command = new ArrayList<>();
        command.add(binary.getAbsolutePath());
        for (String argument : arguments) {
            command.add(argument);
        }

        Process process = new ProcessBuilder(command).redirectErrorStream(false).start();
        // Drain stderr on a separate thread so a full stderr pipe buffer cannot block the
        // process while we read stdout (and vice versa).
        StreamCollector stderrCollector = new StreamCollector(process.getErrorStream());
        Thread stderrThread = new Thread(stderrCollector, "native-tester-stderr");
        stderrThread.setDaemon(true);
        stderrThread.start();

        String stdout = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        boolean finished = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        stderrThread.join(TimeUnit.SECONDS.toMillis(10));
        String stderr = stderrCollector.toString();

        if (!finished) {
            Assert.fail("Native process timed out after " + PROCESS_TIMEOUT_SECONDS + "s.\n"
                    + new ProcessResult(-1, stdout, stderr));
        }
        return new ProcessResult(process.exitValue(), stdout, stderr);
    }

    private File locateNativeBinary() {
        // The Gradle build passes the absolute path of the GraalVM nativeCompile output.
        String binaryPath = System.getProperty("native.image.binary");
        if (binaryPath != null && !binaryPath.isEmpty()) {
            Path explicit = Path.of(binaryPath);
            if (Files.exists(explicit)) {
                return explicit.toFile();
            }
            Path explicitWindows = Path.of(binaryPath + ".exe");
            if (Files.exists(explicitWindows)) {
                return explicitWindows.toFile();
            }
            return explicit.toFile();
        }
        String buildDir = System.getProperty("native.image.directory", "build/native/nativeCompile");
        String imageName = System.getProperty("native.image.name", "pulsar-client-native-tester");
        Path path = Path.of(buildDir, imageName);
        // GraalVM appends .exe on Windows.
        if (!Files.exists(path)) {
            Path windowsPath = Path.of(buildDir, imageName + ".exe");
            if (Files.exists(windowsPath)) {
                return windowsPath.toFile();
            }
        }
        return path.toFile();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public final void cleanup() {
        markCurrentSetupNumberCleaned();
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    /** Collects an input stream's full content as UTF-8 text on a background thread. */
    private static final class StreamCollector implements Runnable {
        private final java.io.InputStream stream;
        private volatile String content = "";

        StreamCollector(java.io.InputStream stream) {
            this.stream = stream;
        }

        @Override
        public void run() {
            try {
                content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (Exception e) {
                content = "<failed to read stderr: " + e + ">";
            }
        }

        @Override
        public String toString() {
            return content;
        }
    }

    private static final class ProcessResult {
        final int exitCode;
        final String stdout;
        final String stderr;

        ProcessResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        @Override
        public String toString() {
            return "exitCode=" + exitCode + "\n--- stdout ---\n" + stdout + "\n--- stderr ---\n" + stderr;
        }
    }
}
