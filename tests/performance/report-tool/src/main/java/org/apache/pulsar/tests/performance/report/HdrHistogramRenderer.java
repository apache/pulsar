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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.imageio.ImageIO;
import org.HdrHistogram.EncodableHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.HistogramLogReader;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.markers.SeriesMarkers;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Plots the latencies of an IoT performance run from its HDR histogram logs, the way HistogramLogAnalyzer does, as
 * two PNG charts drawn with XChart: the latency at each percentile, on an axis that spreads the tail (90 %, 99 %,
 * 99.9 %, …), and the maximum latency of each logged interval over the run. The publish latency and each consumer
 * application's end-to-end latency are separate lines: the applications consume independently, so a merged
 * distribution would describe none of them.
 */
@Command(name = "render-hdr-histograms", mixinStandardHelpOptions = true,
        description = "Plot IoT publish and per-application end-to-end latencies by percentile and over time as PNG")
public final class HdrHistogramRenderer implements Callable<Integer> {
    static final String PERCENTILES_SUFFIX = "-percentiles.png";
    static final String TIMELINE_SUFFIX = "-timeline.png";
    /** The percentile distribution's file extension, as HdrHistogram's plotter, plotFiles.html, reads it. */
    static final String DISTRIBUTION_EXTENSION = ".hgrm";
    private static final int HEIGHT = 620;
    private static final int FOOTER_STRIP_HEIGHT = 24;
    // The percentile axis ends here: 1/(1 - p) is a million, six nines
    private static final double MAX_PERCENTILE = 99.9999;
    private static final int PERCENTILE_TICKS_PER_HALF_DISTANCE = 5;

    @Option(names = "--run-directory", required = true,
            description = "IoT run directory containing producer/ and one directory per consumer application")
    private Path runDirectory;

    @Option(names = "--output-prefix",
            description = "Output path without the -percentiles.png and -timeline.png suffixes; defaults to"
                    + " <run-directory>/latency")
    private Path outputPrefix;

    private HdrHistogramRenderer() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new HdrHistogramRenderer()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        Path normalizedRun = runDirectory.toAbsolutePath().normalize();
        Path producer = normalizedRun.resolve("producer/produce-latency.hdr");
        if (!Files.isRegularFile(producer)) {
            throw new IllegalArgumentException("Producer histogram does not exist: " + producer);
        }
        // The applications' directories are named after their subscriptions, from the run's resolved scenario
        Path resolvedConfig = normalizedRun.resolve(RunReport.RESOLVED_CONFIG);
        JsonNode workload = Files.isRegularFile(resolvedConfig)
                ? new YAMLMapper().readTree(resolvedConfig.toFile()).path("workloads").path("iotTelemetry")
                : MissingNode.getInstance();
        List<Path> consumers = new ArrayList<>();
        List<String> applications = new ArrayList<>();
        for (int application = 0; ; application++) {
            Path directory = RunReport.applicationDirectory(normalizedRun, workload, application);
            Path consumer = directory.resolve("consume-latency.hdr");
            if (!Files.isRegularFile(consumer)) {
                break;
            }
            consumers.add(consumer);
            applications.add(directory.getFileName().toString());
        }
        Path prefix = outputPrefix != null ? outputPrefix.toAbsolutePath().normalize()
                : normalizedRun.resolve("latency");
        // Without the run report's measurement start, time runs from the first logged interval
        long origin = Long.MAX_VALUE;
        for (Path log : concat(producer, consumers)) {
            for (Interval interval : readIntervals(log)) {
                origin = Math.min(origin, interval.startEpochMillis());
            }
        }
        for (Path chart : render(producer, consumers, applications, prefix, origin, "")) {
            System.out.println(chart);
        }
        return 0;
    }

    /**
     * Plots {@code <outputPrefix>-percentiles.png} and {@code <outputPrefix>-timeline.png}.
     *
     * @param consumers each consumer application's end-to-end latency log, in application order
     * @param applications the applications' names, one per log in {@code consumers}
     * @param originEpochMillis the time that the timeline counts seconds from, such as the measurement start
     * @param footer small text at the bottom right, such as the branch, commit and run time; empty for none
     * @return the two charts
     */
    static List<Path> render(Path producer, List<Path> consumers, List<String> applications, Path outputPrefix,
                             long originEpochMillis, String footer) throws IOException {
        List<Path> logs = concat(producer, consumers);
        // Short, so that the legend fits on one row; the chart and the report say what each latency measures
        List<String> names = new ArrayList<>();
        names.add("Publish");
        names.addAll(applications);
        XYChart percentiles = chart("Latency by percentile", "Percentile", HEIGHT);
        percentiles.getStyler().setXAxisLogarithmic(true).setXAxisMin(1.0)
                .setXAxisMax(percentileAxisPosition(MAX_PERCENTILE))
                .setXAxisTickLabelsFormattingFunction(HdrHistogramRenderer::percentileAxisLabel);
        XYChart timeline = chart("Maximum latency per interval", "Seconds since the measurement start", HEIGHT);
        for (int index = 0; index < logs.size(); index++) {
            String name = names.get(index);
            List<Double> positions = new ArrayList<>();
            List<Double> latencies = new ArrayList<>();
            for (HistogramIterationValue value : readMerged(List.of(logs.get(index)))
                    .percentiles(PERCENTILE_TICKS_PER_HALF_DISTANCE)) {
                double percentile = Math.min(value.getPercentileLevelIteratedTo(), MAX_PERCENTILE);
                positions.add(percentileAxisPosition(percentile));
                latencies.add(value.getValueIteratedTo() / 1000.0);
                if (percentile >= MAX_PERCENTILE) {
                    break;
                }
            }
            style(percentiles.addSeries(name, positions, latencies), index);
            List<Double> seconds = new ArrayList<>();
            List<Double> maxima = new ArrayList<>();
            for (Interval interval : readIntervals(logs.get(index))) {
                seconds.add((interval.endEpochMillis() - originEpochMillis) / 1000.0);
                maxima.add(interval.maxMicros() / 1000.0);
            }
            if (!seconds.isEmpty()) {
                style(timeline.addSeries(name, seconds, maxima), index);
            }
        }
        Path parent = outputPrefix.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path percentilesFile = outputPrefix.resolveSibling(outputPrefix.getFileName() + PERCENTILES_SUFFIX);
        Path timelineFile = outputPrefix.resolveSibling(outputPrefix.getFileName() + TIMELINE_SUFFIX);
        writePng(percentiles, footer, percentilesFile);
        writePng(timeline, footer, timelineFile);
        return List.of(percentilesFile, timelineFile);
    }

    /**
     * Writes the log's percentile distribution in milliseconds beside it, {@code produce-latency.hdr} to
     * {@code produce-latency.hgrm}, the text that HdrHistogram's plotFiles.html and other tools plot.
     *
     * @return the distribution file
     */
    static Path writePercentileDistribution(Path log) throws IOException {
        String name = log.getFileName().toString();
        Path distribution = log.resolveSibling((name.endsWith(".hdr") ? name.substring(0, name.length() - 4) : name)
                + DISTRIBUTION_EXTENSION);
        try (PrintStream out = new PrintStream(Files.newOutputStream(distribution), false, "UTF-8")) {
            readMerged(List.of(log)).outputPercentileDistribution(out, 1000.0);
        }
        return distribution;
    }

    /** Where percentile {@code p} (0 to 100) sits on the percentile axis: 1/(1 - p), so each nine is a decade. */
    static double percentileAxisPosition(double percentile) {
        return 1.0 / (1.0 - percentile / 100.0);
    }

    /** The label of a percentile axis position: 1 is 0 %, 10 is 90 %, 100 is 99 %, 1000 is 99.9 %. */
    static String percentileAxisLabel(double position) {
        double percentile = 100.0 * (1.0 - 1.0 / position);
        return BigDecimal.valueOf(percentile).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString()
                + "%";
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

    /** One logged interval: when it started and ended, and the largest latency recorded in it. */
    record Interval(long startEpochMillis, long endEpochMillis, long maxMicros) {
    }

    static List<Interval> readIntervals(Path log) throws IOException {
        List<Interval> intervals = new ArrayList<>();
        try (HistogramLogReader reader = new HistogramLogReader(log.toFile())) {
            EncodableHistogram interval;
            while ((interval = reader.nextIntervalHistogram()) != null) {
                if (!(interval instanceof Histogram histogram)) {
                    throw new IOException("Unsupported HDR histogram type in " + log);
                }
                if (histogram.getTotalCount() > 0) {
                    intervals.add(new Interval(histogram.getStartTimeStamp(), histogram.getEndTimeStamp(),
                            histogram.getMaxValue()));
                }
            }
        }
        return intervals;
    }

    // Publish first, then each consumer application in order, so that the colors match the other charts
    private static List<Path> concat(Path producer, List<Path> consumers) {
        List<Path> logs = new ArrayList<>();
        logs.add(producer);
        logs.addAll(consumers);
        return logs;
    }

    private static XYChart chart(String title, String xAxisTitle, int height) {
        XYChart chart = new XYChartBuilder().width(ChartStyle.WIDTH).height(height).title(title)
                .xAxisTitle(xAxisTitle).yAxisTitle("Latency (ms)").build();
        Styler styler = chart.getStyler();
        styler.setChartBackgroundColor(Color.WHITE).setPlotBackgroundColor(Color.WHITE).setPlotBorderVisible(false)
                .setChartFontColor(ChartStyle.INK).setChartTitleFont(new Font(Font.SANS_SERIF, Font.BOLD, 24))
                .setChartTitleBoxBackgroundColor(Color.WHITE).setLegendFont(new Font(Font.SANS_SERIF, Font.PLAIN, 14))
                .setLegendPosition(Styler.LegendPosition.OutsideS).setLegendLayout(Styler.LegendLayout.Horizontal)
                .setLegendBorderColor(Color.WHITE).setChartPadding(24).setAntiAlias(true).setTextAntiAlias(true);
        chart.getStyler().setPlotGridLinesColor(ChartStyle.GRID)
                .setAxisTickLabelsFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12))
                .setAxisTitleFont(new Font(Font.SANS_SERIF, Font.PLAIN, 13))
                .setAxisTickMarksColor(ChartStyle.MUTED).setYAxisMin(0.0);
        chart.getStyler().setMarkerSize(0);
        return chart;
    }

    // Publish is dotted, as the producers are in the throughput chart; the applications' lines are solid
    private static void style(XYSeries series, int index) {
        series.setMarker(SeriesMarkers.NONE);
        series.setLineColor(ChartStyle.seriesColor(index));
        series.setLineStyle(index == 0 ? ChartStyle.DOTTED : new BasicStroke(2f));
    }

    // The footer gets a strip of its own below the chart, where XChart's legend cannot overlap it
    private static void writePng(XYChart chart, String footer, Path output) throws IOException {
        BufferedImage chartImage = BitmapEncoder.getBufferedImage(chart);
        BufferedImage image = new BufferedImage(chartImage.getWidth(),
                chartImage.getHeight() + (footer.isEmpty() ? 0 : FOOTER_STRIP_HEIGHT), BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = image.createGraphics();
        try {
            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, image.getWidth(), image.getHeight());
            graphics.drawImage(chartImage, 0, 0, null);
            graphics.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
            ChartStyle.drawFooter(graphics, footer, image.getHeight());
        } finally {
            graphics.dispose();
        }
        ImageIO.write(image, "png", output.toFile());
    }
}
