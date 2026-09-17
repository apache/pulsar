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
package org.apache.pulsar.tests.integration.containers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBufAllocator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.tests.ExtendedNettyLeakDetector;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NettyLeakDetectionTest {
    @DataProvider
    public Object[][] shutdownModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "shutdownModes")
    public void collectsLeaksReportedDuringShutdown(boolean supervised) throws Exception {
        if (!ExtendedNettyLeakDetector.isExtendedNettyLeakDetectorEnabled()
                || !"paranoid".equals(System.getProperty("io.netty.leakDetection.level"))) {
            throw new SkipException("Requires the default paranoid test leak detector");
        }
        var container = new LeakProbeContainer(supervised);
        Path logs = Path.of(System.getProperty("buildDirectory", "build"),
                "container-logs", container.getContainerName());
        try (container) {
            container.start();
            assertThat(container.execCmd("sh", "-c", "ls /var/log/pulsar/netty_leak_*.txt 2>/dev/null || true")
                    .getStdout()).as("No leak report before JVM shutdown").isEmpty();
            container.stop();
            Path archive = logs.resolve("var-log-pulsar.tar.gz");
            boolean foundLeak = false;
            try (var tar = new TarArchiveInputStream(new GZIPInputStream(Files.newInputStream(archive)))) {
                TarArchiveEntry entry;
                while ((entry = tar.getNextEntry()) != null) {
                    if (entry.isFile() && entry.getName().contains("netty_leak_")) {
                        String report = new String(tar.readAllBytes(), UTF_8);
                        assertThat(report).contains("Traced leak detected ByteBuf", "container-shutdown-leak");
                        foundLeak = true;
                    }
                }
            }
            assertThat(foundLeak).as("Shutdown leak included in the collected container logs").isTrue();
        } finally {
            // This test deliberately leaks in a separate JVM. Do not report its expected leak in CI.
            FileUtils.deleteDirectory(logs.toFile());
        }
    }

    private static class LeakProbeContainer extends PulsarContainer<LeakProbeContainer> {
        LeakProbeContainer(boolean supervised) {
            super("leak-test-" + UUID.randomUUID(), "probe", "probe",
                    supervised ? "/usr/bin/supervisord" : "bin/pulsar", INVALID_PORT, INVALID_PORT);
            String className = LeakProbe.class.getName();
            String resource = className.replace('.', '/') + ".class";
            withCopyFileToContainer(MountableFile.forClasspathResource(resource), "/tmp/" + resource);
            String script = "#!/bin/sh\nexec java $PULSAR_EXTRA_OPTS -cp '/pulsar/lib/*:/tmp' '"
                    + className + "'\n";
            if (supervised) {
                withCopyToContainer(Transferable.of(script, 0755), "/tmp/leak-probe.sh");
                withCopyToContainer(Transferable.of("""
                        [program:leak-probe]
                        command=/tmp/leak-probe.sh
                        autostart=true
                        autorestart=false
                        stopwaitsecs=15
                        """), "/etc/supervisord/conf.d/leak-probe.conf");
                withCommand("-c", "/etc/supervisord.conf");
            } else {
                // Use the standalone shutdown path with a small JVM instead of starting a broker.
                withCopyToContainer(Transferable.of(script, 0755), "/pulsar/bin/pulsar");
                withCommand();
            }
            waitingFor(Wait.forSuccessfulCommand("test -f /tmp/leak-probe-ready")
                    .withStartupTimeout(Duration.ofSeconds(60)));
        }

        @Override
        protected void passNettyLeakDetectionSystemProperties() {
            super.passNettyLeakDetectionSystemProperties();
            // Keep the deliberate leak alive until shutdown even when local tests fail on leaks.
            appendToEnv("PULSAR_EXTRA_OPTS",
                    "-D" + ExtendedNettyLeakDetector.EXIT_JVM_ON_LEAK_SYSTEM_PROPERTY_NAME + "=false");
        }
    }

    public static class LeakProbe {
        public static void main(String[] args) throws Exception {
            ExtendedNettyLeakDetector.setInitialHint("container-shutdown-leak");
            leakBuffer();
            Files.writeString(Path.of("/tmp/leak-probe-ready"), "ready");
            // Only the detector's shutdown hook will force collection and report the leaked buffer.
            new CountDownLatch(1).await();
        }

        private static void leakBuffer() {
            ByteBufAllocator.DEFAULT.directBuffer(16).writeLong(42);
        }
    }
}
