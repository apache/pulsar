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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MarkdownPagesTest {
    private Path directory;

    @BeforeMethod
    public void createDirectory() throws IOException {
        directory = Files.createTempDirectory("markdown-pages-test").toRealPath();
    }

    @AfterMethod(alwaysRun = true)
    public void deleteDirectory() throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    @DataProvider
    public Object[][] destinations() {
        Path root = Path.of("/runs/run-1");
        Path page = root.resolve("broker-profile");
        return new Object[][] {
                {"profile-report.md", page, root, "profile-report.html"},
                {"broker-offcpu/jonoffcpu-summary.md#capture", page, root,
                        "broker-offcpu/jonoffcpu-summary.html#capture"},
                {"/runs/run-1/broker-profile/b.offcpu-idle-waits.txt", page, root, "b.offcpu-idle-waits.txt"},
                {"/runs/run-1/run-report.md", page, root, "../run-report.html"},
                {"/elsewhere/file.txt", page, root, "/elsewhere/file.txt"},
                {"https://github.com/jonoffcpu/jonoffcpu/README.md", page, root,
                        "https://github.com/jonoffcpu/jonoffcpu/README.md"},
                {"#where-the-time-went", page, root, "#where-the-time-went"},
                {"broker-flamegraphs/cpu.html", page, root, "broker-flamegraphs/cpu.html"},
        };
    }

    @Test(dataProvider = "destinations")
    public void rewritesLinksToPagesAndRunFiles(String destination, Path pageDirectory, Path root, String expected) {
        assertEquals(MarkdownPages.rewrite(destination, pageDirectory, root), expected);
    }

    @DataProvider
    public Object[][] javaNames() {
        return new Object[][] {
                {"org.apache.pulsar.broker.service.Consumer.sendMessages", "o.a.p.b.s.Consumer.sendMessages"},
                {"org.apache.pulsar.broker.service.ServerCnx.handleSend → … (4) → java.util.concurrent.locks"
                        + ".StampedLock.readLock",
                        "o.a.p.b.s.ServerCnx.handleSend → … (4) → j.u.c.l.StampedLock.readLock"},
                {"org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap$Section.get",
                        "o.a.b.u.c.ConcurrentOpenHashMap$Section.get"},
                {"libjvm.so.Unsafe_Park", "libjvm.so.Unsafe_Park"},
                {"C2 Runtime complete_monitor_locking", "C2 Runtime complete_monitor_locking"},
                {"^org\\.apache\\.", "^org\\.apache\\."},
                {"/runs/run-1/broker.offcpu-idle-waits.txt", "/runs/run-1/broker.offcpu-idle-waits.txt"},
                {"ZDriverMinor", "ZDriverMinor"},
        };
    }

    @Test(dataProvider = "javaNames")
    public void abbreviatesJavaPackages(String text, String expected) {
        assertEquals(MarkdownPages.abbreviateJavaNames(text), expected);
    }

    @Test
    public void abbreviatesJavaNamesWithTheFullNameAsTooltip() throws IOException {
        Path markdown = directory.resolve("jonoffcpu-summary.md");
        Files.writeString(markdown, "| Boundary |\n|---|\n| `org.apache.pulsar.broker.service.Consumer.sendMessages` |"
                + "\n\n<details><summary>2: `org.apache.pulsar.common.protocol.PulsarDecoder.channelRead`</summary>"
                + "\n\n1. `org.apache.pulsar.common.protocol.PulsarDecoder.channelRead`\n\n</details>\n");

        String html = Files.readString(MarkdownPages.renderHtml(markdown, directory, "Digest", true));

        assertTrue(html.contains("<code title=\"org.apache.pulsar.broker.service.Consumer.sendMessages\">"
                + "o.a.p.b.s.Consumer.sendMessages</code>"), html);
        assertTrue(html.contains("<summary>2: `o.a.p.c.p.PulsarDecoder.channelRead`</summary>"), html);
        // Other pages keep the names as written
        String full = Files.readString(MarkdownPages.renderHtml(markdown, directory, "Digest"));
        assertTrue(full.contains("<code>org.apache.pulsar.broker.service.Consumer.sendMessages</code>"), full);
    }

    @Test
    public void givesHeadingsIdsForInPageLinks() throws IOException {
        Path markdown = directory.resolve("jonoffcpu-summary.md");
        Files.writeString(markdown, "# jonoffcpu analysis digest\n\nSee [the totals](#where-the-time-went).\n\n"
                + "## Where the time went\n\n## About this digest\n");

        String html = Files.readString(MarkdownPages.renderHtml(markdown, directory, "Digest"));

        assertTrue(html.contains("<h2 id=\"where-the-time-went\">Where the time went</h2>"), html);
        assertTrue(html.contains("<h2 id=\"about-this-digest\">About this digest</h2>"), html);
        assertTrue(html.contains("<a href=\"#where-the-time-went\">the totals</a>"), html);
    }

    @Test
    public void rendersTablesLinksAndTheStylesheet() throws IOException {
        Path profile = Files.createDirectories(directory.resolve("broker-profile"));
        Path markdown = profile.resolve("profile-report.md");
        Files.writeString(markdown, "# Report\n\n| Slice | s |\n|---|---:|\n"
                + "| [no idle](offcpu-no-idle.html) | 3.7 |\n\n"
                + "[digest](broker-offcpu/jonoffcpu-summary.md) and [patterns](" + profile.resolve("idle.txt")
                + ")\n\n![chart](../latency-histograms.svg)\n");

        Path page = MarkdownPages.renderHtml(markdown, directory, "Profile <report>");

        assertEquals(page, profile.resolve("profile-report.html"));
        String html = Files.readString(page);
        assertTrue(html.contains("<title>Profile &lt;report&gt;</title>"), html);
        assertTrue(html.contains("<table>"), html);
        assertTrue(html.contains("<td align=\"right\">3.7</td>"), html);
        assertTrue(html.contains("<a href=\"offcpu-no-idle.html\">no idle</a>"), html);
        assertTrue(html.contains("<a href=\"broker-offcpu/jonoffcpu-summary.html\">digest</a>"), html);
        assertTrue(html.contains("<a href=\"idle.txt\">patterns</a>"), html);
        assertTrue(html.contains("<img src=\"../latency-histograms.svg\" alt=\"chart\" />"), html);
        assertTrue(html.contains("prefers-color-scheme"), html);
    }
}
