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
package org.apache.pulsar.tests.performance.report;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import javax.imageio.ImageIO;
import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.HistogramLogReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Renders the producer and merged consumer HDR latency distributions from an IoT performance run. */
@Command(name = "render-hdr-histograms", mixinStandardHelpOptions = true,
        description = "Render IoT producer and consumer HDR latency distributions as PNG and SVG")
public final class HdrHistogramRenderer implements Callable<Integer> {
    static final int WIDTH = 1400;
    private static final int HEIGHT = 700;
    private static final int PLOT_TOP = 125;
    private static final int PLOT_HEIGHT = 390;
    private static final int PANEL_WIDTH = 590;
    private static final int FIRST_PANEL_X = 100;
    private static final int SECOND_PANEL_X = 760;
    private static final int BIN_COUNT = 50;
    static final Color INK = new Color(20, 43, 64);
    static final Color MUTED = new Color(80, 98, 117);
    static final Color GRID = new Color(218, 225, 232);
    static final Color PRODUCER = new Color(0, 123, 155);
    static final Color CONSUMER = new Color(189, 91, 36);
    // The footer that says which run a chart shows: small, as it is read only when needed
    private static final int FOOTER_FONT_SIZE = 10;
    private static final int FOOTER_MARGIN = 12;

    @Option(names = "--run-directory", required = true,
            description = "IoT run directory containing producer/ and consumer-* outputs")
    private Path runDirectory;

    @Option(names = "--output-prefix",
            description = "Output path without extension; defaults to <run-directory>/latency-histograms")
    private Path outputPrefix;

    @Option(names = "--title", defaultValue = "IoT telemetry latency",
            description = "Chart title")
    private String title;

    private HdrHistogramRenderer() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new HdrHistogramRenderer()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        Path normalizedRun = runDirectory.toAbsolutePath().normalize();
        Path producer = normalizedRun.resolve("producer/produce-latency.hdr");
        List<Path> consumers;
        try (var files = Files.walk(normalizedRun)) {
            consumers = files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().equals("consume-latency.hdr"))
                    .sorted()
                    .toList();
        }
        if (!Files.isRegularFile(producer)) {
            throw new IllegalArgumentException("Producer histogram does not exist: " + producer);
        }
        if (consumers.isEmpty()) {
            throw new IllegalArgumentException("No consumer histograms found below " + normalizedRun);
        }
        Path prefix = outputPrefix != null
                ? outputPrefix.toAbsolutePath().normalize()
                : normalizedRun.resolve("latency-histograms");
        render(producer, consumers, prefix, title);
        System.out.println(prefix + ".png");
        System.out.println(prefix + ".svg");
        return 0;
    }

    /** Merges all intervals for each role and renders count-weighted latency distributions. */
    public static void render(Path producer, List<Path> consumers, Path outputPrefix, String title)
            throws IOException {
        render(producer, consumers, outputPrefix, title, "");
    }

    /**
     * Merges all intervals for each role and renders count-weighted latency distributions.
     *
     * @param footer small text at the bottom right, such as the branch, commit and run time; empty for none
     */
    static void render(Path producer, List<Path> consumers, Path outputPrefix, String title, String footer)
            throws IOException {
        Dataset producerData = dataset("Produce · send completion", readMerged(List.of(producer)), PRODUCER);
        Dataset consumerData = dataset("Consume · publish to listener", readMerged(consumers), CONSUMER);
        Path parent = outputPrefix.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writePng(outputPrefix.resolveSibling(outputPrefix.getFileName() + ".png"), title,
                producerData, consumerData, footer);
        Files.writeString(outputPrefix.resolveSibling(outputPrefix.getFileName() + ".svg"),
                svg(title, producerData, consumerData, footer));
    }

    /** Draws {@code footer} at the bottom right in a small font, which can be zoomed into. */
    static void drawFooter(Graphics2D graphics, String footer, int height) {
        if (footer.isEmpty()) {
            return;
        }
        graphics.setColor(MUTED);
        graphics.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, FOOTER_FONT_SIZE));
        graphics.drawString(footer, WIDTH - FOOTER_MARGIN - graphics.getFontMetrics().stringWidth(footer),
                height - FOOTER_MARGIN);
    }

    /** The SVG form of {@link #drawFooter(Graphics2D, String, int)}. */
    static void appendSvgFooter(StringBuilder out, String footer, int height) {
        if (footer.isEmpty()) {
            return;
        }
        out.append("<text class=\"muted\" x=\"").append(WIDTH - FOOTER_MARGIN).append("\" y=\"")
                .append(height - FOOTER_MARGIN).append("\" text-anchor=\"end\" font-size=\"")
                .append(FOOTER_FONT_SIZE).append("\">").append(xml(footer)).append("</text>\n");
    }

    static Histogram readMerged(List<Path> paths) throws IOException {
        Histogram merged = null;
        for (Path path : paths) {
            try (HistogramLogReader reader = new HistogramLogReader(path.toFile())) {
                EncodableHistogram interval;
                while ((interval = reader.nextIntervalHistogram()) != null) {
                    if (!(interval instanceof Histogram histogram)) {
                        throw new IOException("Unsupported HDR histogram type in " + path);
                    }
                    if (merged == null) {
                        merged = histogram.copy();
                    } else {
                        merged.add(histogram);
                    }
                }
            }
        }
        if (merged == null || merged.getTotalCount() == 0) {
            throw new IOException("No HDR latency observations in " + paths);
        }
        return merged;
    }

    private static Dataset dataset(String name, Histogram histogram, Color color) {
        long minPositive = Long.MAX_VALUE;
        long max = 1;
        for (HistogramIterationValue value : histogram.recordedValues()) {
            long representative = histogram.medianEquivalentValue(value.getValueIteratedTo());
            if (representative > 0) {
                minPositive = Math.min(minPositive, representative);
            }
            max = Math.max(max, representative);
        }
        if (minPositive == Long.MAX_VALUE) {
            minPositive = 1;
        }
        double minMillis = Math.max(0.001, minPositive / 1000.0);
        double maxMillis = Math.max(minMillis * 1.01, max / 1000.0);
        double logMin = Math.log10(minMillis);
        double logMax = Math.log10(maxMillis);
        long[] bins = new long[BIN_COUNT];
        for (HistogramIterationValue value : histogram.recordedValues()) {
            double millis = Math.max(minMillis,
                    histogram.medianEquivalentValue(value.getValueIteratedTo()) / 1000.0);
            int bin = (int) ((Math.log10(millis) - logMin) / (logMax - logMin) * BIN_COUNT);
            bins[Math.max(0, Math.min(BIN_COUNT - 1, bin))] += value.getCountAtValueIteratedTo();
        }
        long peak = 1;
        for (long count : bins) {
            peak = Math.max(peak, count);
        }
        return new Dataset(name, histogram, color, bins, minMillis, maxMillis, peak);
    }

    private static void writePng(Path output, String title, Dataset first, Dataset second, String footer)
            throws IOException {
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = image.createGraphics();
        try {
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, WIDTH, HEIGHT);
            graphics.setColor(INK);
            graphics.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 28));
            graphics.drawString(title, 70, 55);
            drawDataset(graphics, first, FIRST_PANEL_X);
            drawDataset(graphics, second, SECOND_PANEL_X);
            drawFooter(graphics, footer, HEIGHT);
        } finally {
            graphics.dispose();
        }
        ImageIO.write(image, "png", output.toFile());
    }

    private static void drawDataset(Graphics2D graphics, Dataset data, int x) {
        graphics.setColor(INK);
        graphics.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 19));
        graphics.drawString(data.name(), x, PLOT_TOP - 28);
        graphics.setStroke(new BasicStroke(1));
        graphics.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
        for (int line = 0; line <= 4; line++) {
            int y = PLOT_TOP + PLOT_HEIGHT - line * PLOT_HEIGHT / 4;
            graphics.setColor(GRID);
            graphics.drawLine(x, y, x + PANEL_WIDTH, y);
            graphics.setColor(MUTED);
            graphics.drawString(String.format(Locale.ROOT, "%.1f%%", data.peakPercent() * line / 4),
                    x - 50, y + 4);
        }
        double barWidth = (double) PANEL_WIDTH / BIN_COUNT;
        graphics.setColor(new Color(data.color().getRed(), data.color().getGreen(), data.color().getBlue(), 190));
        for (int bin = 0; bin < BIN_COUNT; bin++) {
            int height = (int) Math.round(PLOT_HEIGHT * data.bins()[bin] / (double) data.peakCount());
            int barX = x + (int) Math.floor(bin * barWidth);
            int nextX = x + (int) Math.floor((bin + 1) * barWidth);
            graphics.fillRect(barX, PLOT_TOP + PLOT_HEIGHT - height, Math.max(1, nextX - barX - 1), height);
        }
        graphics.setColor(INK);
        graphics.drawLine(x, PLOT_TOP + PLOT_HEIGHT, x + PANEL_WIDTH, PLOT_TOP + PLOT_HEIGHT);
        graphics.drawString(formatMillis(data.minMillis()), x, PLOT_TOP + PLOT_HEIGHT + 22);
        String max = formatMillis(data.maxMillis());
        int maxWidth = graphics.getFontMetrics().stringWidth(max);
        graphics.drawString(max, x + PANEL_WIDTH - maxWidth, PLOT_TOP + PLOT_HEIGHT + 22);
        graphics.drawString("Latency (ms, logarithmic)", x + PANEL_WIDTH / 2 - 75, PLOT_TOP + PLOT_HEIGHT + 38);
        graphics.setColor(data.color());
        graphics.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        graphics.drawString(summary(data.histogram()), x, PLOT_TOP + PLOT_HEIGHT + 82);
    }

    private static String svg(String title, Dataset first, Dataset second, String footer) {
        StringBuilder out = new StringBuilder(32_000);
        out.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(WIDTH)
                .append("\" height=\"").append(HEIGHT).append("\" viewBox=\"0 0 ").append(WIDTH).append(' ')
                .append(HEIGHT).append("\">\n<rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n")
                .append("<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#142b40}.muted{fill:#506275}"
                        + ".summary{font-family:DejaVu Sans Mono,monospace}</style>\n")
                .append("<text x=\"70\" y=\"55\" font-size=\"28\" font-weight=\"bold\">")
                .append(xml(title)).append("</text>\n");
        appendSvgDataset(out, first, FIRST_PANEL_X);
        appendSvgDataset(out, second, SECOND_PANEL_X);
        appendSvgFooter(out, footer, HEIGHT);
        return out.append("</svg>\n").toString();
    }

    private static void appendSvgDataset(StringBuilder out, Dataset data, int x) {
        out.append("<text x=\"").append(x).append("\" y=\"").append(PLOT_TOP - 28)
                .append("\" font-size=\"19\" font-weight=\"bold\">").append(xml(data.name())).append("</text>\n");
        for (int line = 0; line <= 4; line++) {
            int y = PLOT_TOP + PLOT_HEIGHT - line * PLOT_HEIGHT / 4;
            out.append("<line x1=\"").append(x).append("\" y1=\"").append(y).append("\" x2=\"")
                    .append(x + PANEL_WIDTH).append("\" y2=\"").append(y)
                    .append("\" stroke=\"#dae1e8\"/>\n<text class=\"muted\" x=\"").append(x - 50)
                    .append("\" y=\"").append(y + 4).append("\" font-size=\"12\">")
                    .append(String.format(Locale.ROOT, "%.1f%%", data.peakPercent() * line / 4))
                    .append("</text>\n");
        }
        double barWidth = (double) PANEL_WIDTH / BIN_COUNT;
        String color = String.format(Locale.ROOT, "#%02x%02x%02x", data.color().getRed(),
                data.color().getGreen(), data.color().getBlue());
        for (int bin = 0; bin < BIN_COUNT; bin++) {
            int height = (int) Math.round(PLOT_HEIGHT * data.bins()[bin] / (double) data.peakCount());
            int barX = x + (int) Math.floor(bin * barWidth);
            int nextX = x + (int) Math.floor((bin + 1) * barWidth);
            out.append("<rect x=\"").append(barX).append("\" y=\"")
                    .append(PLOT_TOP + PLOT_HEIGHT - height).append("\" width=\"")
                    .append(Math.max(1, nextX - barX - 1)).append("\" height=\"").append(height)
                    .append("\" fill=\"").append(color).append("\" fill-opacity=\"0.75\"/>\n");
        }
        int bottom = PLOT_TOP + PLOT_HEIGHT;
        out.append("<line x1=\"").append(x).append("\" y1=\"").append(bottom).append("\" x2=\"")
                .append(x + PANEL_WIDTH).append("\" y2=\"").append(bottom).append("\" stroke=\"#142b40\"/>\n")
                .append("<text x=\"").append(x).append("\" y=\"").append(bottom + 22)
                .append("\" font-size=\"12\">").append(formatMillis(data.minMillis())).append("</text>\n")
                .append("<text x=\"").append(x + PANEL_WIDTH).append("\" y=\"").append(bottom + 22)
                .append("\" text-anchor=\"end\" font-size=\"12\">").append(formatMillis(data.maxMillis()))
                .append("</text>\n<text x=\"").append(x + PANEL_WIDTH / 2).append("\" y=\"")
                .append(bottom + 38).append("\" text-anchor=\"middle\" font-size=\"12\">"
                        + "Latency (ms, logarithmic)</text>\n")
                .append("<text class=\"summary\" x=\"").append(x).append("\" y=\"").append(bottom + 82)
                .append("\" font-size=\"13\" fill=\"").append(color).append("\">")
                .append(xml(summary(data.histogram()))).append("</text>\n");
    }

    private static String summary(Histogram histogram) {
        return String.format(Locale.ROOT, "n=%,d  p50=%s  p95=%s  p99=%s  p99.9=%s",
                histogram.getTotalCount(), formatMicros(histogram.getValueAtPercentile(50)),
                formatMicros(histogram.getValueAtPercentile(95)),
                formatMicros(histogram.getValueAtPercentile(99)),
                formatMicros(histogram.getValueAtPercentile(99.9)));
    }

    private static String formatMicros(long micros) {
        return formatMillis(micros / 1000.0);
    }

    private static String formatMillis(double millis) {
        return millis >= 100 ? String.format(Locale.ROOT, "%.0f ms", millis)
                : millis >= 10 ? String.format(Locale.ROOT, "%.1f ms", millis)
                : String.format(Locale.ROOT, "%.3f ms", millis);
    }

    static String xml(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }

    private record Dataset(String name, Histogram histogram, Color color, long[] bins,
                           double minMillis, double maxMillis, long peakCount) {
        double peakPercent() {
            return peakCount * 100.0 / histogram.getTotalCount();
        }
    }
}
