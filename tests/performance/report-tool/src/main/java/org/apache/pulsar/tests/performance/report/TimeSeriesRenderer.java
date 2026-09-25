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

import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.CONSUMER;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.GRID;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.INK;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.MUTED;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.PRODUCER;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.WIDTH;
import static org.apache.pulsar.tests.performance.report.HdrHistogramRenderer.xml;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.imageio.ImageIO;

/**
 * Renders values over time, such as throughput or backlog sampled once per second, as a line chart in PNG and SVG,
 * in the style and width of {@link HdrHistogramRenderer}'s latency chart so that the charts line up in a report.
 * The x axis is seconds since the measurement start: time before it (the warmup) is shaded, and a marker line
 * shows when the producers finished.
 */
final class TimeSeriesRenderer {
    private static final int HEIGHT = 520;
    private static final int PLOT_LEFT = 110;
    private static final int PLOT_RIGHT = WIDTH - 50;
    private static final int PLOT_TOP = 110;
    private static final int PLOT_HEIGHT = 300;
    private static final int PLOT_BOTTOM = PLOT_TOP + PLOT_HEIGHT;
    private static final int GRID_LINES = 5;
    // A legend wider than the plot continues on the next row
    private static final int LEGEND_ROW_HEIGHT = 20;
    private static final Color WARMUP = new Color(236, 240, 244);
    private static final List<Color> COLORS = List.of(PRODUCER, CONSUMER, new Color(96, 70, 160),
            new Color(46, 125, 50), new Color(173, 20, 87), new Color(120, 144, 156));

    /**
     * One line of the chart; a {@link Double#NaN} value leaves a gap.
     *
     * @param dotted whether the line is dotted, so that a line it overlaps stays visible
     */
    record Series(String name, double[] values, boolean dotted) {
        Series(String name, double[] values) {
            this(name, values, false);
        }
    }

    private record Scale(double minSeconds, double maxSeconds, double maxValue) {
        int x(double seconds) {
            return PLOT_LEFT + (int) Math.round((seconds - minSeconds) / (maxSeconds - minSeconds)
                    * (PLOT_RIGHT - PLOT_LEFT));
        }

        int y(double value) {
            return PLOT_BOTTOM - (int) Math.round(value / maxValue * PLOT_HEIGHT);
        }
    }

    private TimeSeriesRenderer() {
    }

    /**
     * Renders {@code <outputPrefix>.png} and {@code <outputPrefix>.svg}.
     *
     * @param seconds the sample times, seconds since the measurement start, ascending
     * @param series the lines, each with one value per sample time
     * @param producersFinishedSeconds where to mark the producers' finish, or {@link Double#NaN} for no marker
     * @param footer small text at the bottom right, such as the branch, commit and run time; empty for none
     */
    static void render(Path outputPrefix, String title, String yLabel, double[] seconds, List<Series> series,
                       double producersFinishedSeconds, String footer) throws IOException {
        if (seconds.length < 2) {
            throw new IllegalArgumentException("A time series chart needs at least two samples");
        }
        double maxValue = 1;
        for (Series line : series) {
            for (double value : line.values()) {
                if (!Double.isNaN(value)) {
                    maxValue = Math.max(maxValue, value);
                }
            }
        }
        Scale scale = new Scale(seconds[0], seconds[seconds.length - 1], axisMaximum(maxValue * 1.05));
        Path parent = outputPrefix.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writePng(outputPrefix.resolveSibling(outputPrefix.getFileName() + ".png"), title, yLabel, seconds, series,
                producersFinishedSeconds, scale, footer);
        Files.writeString(outputPrefix.resolveSibling(outputPrefix.getFileName() + ".svg"),
                svg(title, yLabel, seconds, series, producersFinishedSeconds, scale, footer));
    }

    private static void writePng(Path output, String title, String yLabel, double[] seconds, List<Series> series,
                                 double finished, Scale scale, String footer) throws IOException {
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = image.createGraphics();
        try {
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            graphics.setColor(Color.WHITE);
            graphics.fillRect(0, 0, WIDTH, HEIGHT);
            graphics.setColor(INK);
            graphics.setFont(new Font(Font.SANS_SERIF, Font.BOLD, 28));
            graphics.drawString(title, 70, 55);
            graphics.setColor(MUTED);
            graphics.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 15));
            graphics.drawString(yLabel + ", sampled once per second", 70, 82);
            graphics.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 12));
            if (scale.minSeconds() < 0) {
                graphics.setColor(WARMUP);
                graphics.fillRect(PLOT_LEFT, PLOT_TOP, scale.x(0) - PLOT_LEFT, PLOT_HEIGHT);
                graphics.setColor(MUTED);
                graphics.drawString("warmup", PLOT_LEFT + 6, PLOT_TOP + 14);
            }
            for (int line = 0; line <= GRID_LINES; line++) {
                double value = scale.maxValue() * line / GRID_LINES;
                int y = scale.y(value);
                graphics.setColor(GRID);
                graphics.drawLine(PLOT_LEFT, y, PLOT_RIGHT, y);
                graphics.setColor(MUTED);
                String label = compact(value);
                graphics.drawString(label, PLOT_LEFT - 10 - graphics.getFontMetrics().stringWidth(label), y + 4);
            }
            for (double tick : ticks(scale)) {
                int x = scale.x(tick);
                graphics.setColor(INK);
                graphics.drawLine(x, PLOT_BOTTOM, x, PLOT_BOTTOM + 5);
                String label = String.format(Locale.ROOT, "%.0f", tick);
                graphics.drawString(label, x - graphics.getFontMetrics().stringWidth(label) / 2, PLOT_BOTTOM + 20);
            }
            graphics.drawLine(PLOT_LEFT, PLOT_BOTTOM, PLOT_RIGHT, PLOT_BOTTOM);
            String xLabel = "Seconds since the measurement start";
            graphics.drawString(xLabel, (PLOT_LEFT + PLOT_RIGHT - graphics.getFontMetrics().stringWidth(xLabel)) / 2,
                    PLOT_BOTTOM + 42);
            if (!Double.isNaN(finished)) {
                int x = scale.x(finished);
                graphics.setColor(MUTED);
                graphics.setStroke(new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10,
                        new float[] {5, 4}, 0));
                graphics.drawLine(x, PLOT_TOP, x, PLOT_BOTTOM);
                String label = "producers finished";
                graphics.drawString(label, labelOnLeft(x) ? x - 6 - graphics.getFontMetrics().stringWidth(label)
                        : x + 6, PLOT_TOP + 14);
            }
            for (int index = 0; index < series.size(); index++) {
                graphics.setColor(COLORS.get(index % COLORS.size()));
                graphics.setStroke(stroke(series.get(index)));
                for (int[][] segment : segments(seconds, series.get(index).values(), scale)) {
                    graphics.drawPolyline(segment[0], segment[1], segment[0].length);
                }
            }
            graphics.setFont(new Font(Font.SANS_SERIF, Font.PLAIN, 14));
            int legendX = PLOT_LEFT;
            int legendY = PLOT_BOTTOM + 72;
            for (int index = 0; index < series.size(); index++) {
                int width = 40 + graphics.getFontMetrics().stringWidth(series.get(index).name());
                if (legendX > PLOT_LEFT && legendX + width > PLOT_RIGHT) {
                    legendX = PLOT_LEFT;
                    legendY += LEGEND_ROW_HEIGHT;
                }
                graphics.setColor(COLORS.get(index % COLORS.size()));
                graphics.setStroke(stroke(series.get(index)));
                graphics.drawLine(legendX, legendY - 4, legendX + 18, legendY - 4);
                graphics.setColor(INK);
                graphics.drawString(series.get(index).name(), legendX + 26, legendY);
                legendX += width;
            }
            HdrHistogramRenderer.drawFooter(graphics, footer, HEIGHT);
        } finally {
            graphics.dispose();
        }
        ImageIO.write(image, "png", output.toFile());
    }

    private static String svg(String title, String yLabel, double[] seconds, List<Series> series, double finished,
                              Scale scale, String footer) {
        StringBuilder out = new StringBuilder(16_000);
        out.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(WIDTH).append("\" height=\"")
                .append(HEIGHT).append("\" viewBox=\"0 0 ").append(WIDTH).append(' ').append(HEIGHT)
                .append("\">\n<rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n")
                .append("<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:#142b40}.muted{fill:#506275}"
                        + "</style>\n")
                .append("<text x=\"70\" y=\"55\" font-size=\"28\" font-weight=\"bold\">").append(xml(title))
                .append("</text>\n<text class=\"muted\" x=\"70\" y=\"82\" font-size=\"15\">").append(xml(yLabel))
                .append(", sampled once per second</text>\n");
        if (scale.minSeconds() < 0) {
            out.append("<rect x=\"").append(PLOT_LEFT).append("\" y=\"").append(PLOT_TOP).append("\" width=\"")
                    .append(scale.x(0) - PLOT_LEFT).append("\" height=\"").append(PLOT_HEIGHT)
                    .append("\" fill=\"").append(hex(WARMUP)).append("\"/>\n<text class=\"muted\" x=\"")
                    .append(PLOT_LEFT + 6).append("\" y=\"").append(PLOT_TOP + 14)
                    .append("\" font-size=\"12\">warmup</text>\n");
        }
        for (int line = 0; line <= GRID_LINES; line++) {
            double value = scale.maxValue() * line / GRID_LINES;
            int y = scale.y(value);
            out.append("<line x1=\"").append(PLOT_LEFT).append("\" y1=\"").append(y).append("\" x2=\"")
                    .append(PLOT_RIGHT).append("\" y2=\"").append(y).append("\" stroke=\"").append(hex(GRID))
                    .append("\"/>\n<text class=\"muted\" x=\"").append(PLOT_LEFT - 10).append("\" y=\"")
                    .append(y + 4).append("\" text-anchor=\"end\" font-size=\"12\">").append(compact(value))
                    .append("</text>\n");
        }
        for (double tick : ticks(scale)) {
            int x = scale.x(tick);
            out.append("<line x1=\"").append(x).append("\" y1=\"").append(PLOT_BOTTOM).append("\" x2=\"").append(x)
                    .append("\" y2=\"").append(PLOT_BOTTOM + 5).append("\" stroke=\"#142b40\"/>\n<text x=\"")
                    .append(x).append("\" y=\"").append(PLOT_BOTTOM + 20)
                    .append("\" text-anchor=\"middle\" font-size=\"12\">")
                    .append(String.format(Locale.ROOT, "%.0f", tick)).append("</text>\n");
        }
        out.append("<line x1=\"").append(PLOT_LEFT).append("\" y1=\"").append(PLOT_BOTTOM).append("\" x2=\"")
                .append(PLOT_RIGHT).append("\" y2=\"").append(PLOT_BOTTOM).append("\" stroke=\"#142b40\"/>\n")
                .append("<text x=\"").append((PLOT_LEFT + PLOT_RIGHT) / 2).append("\" y=\"").append(PLOT_BOTTOM + 42)
                .append("\" text-anchor=\"middle\" font-size=\"12\">Seconds since the measurement start</text>\n");
        if (!Double.isNaN(finished)) {
            int x = scale.x(finished);
            out.append("<line x1=\"").append(x).append("\" y1=\"").append(PLOT_TOP).append("\" x2=\"").append(x)
                    .append("\" y2=\"").append(PLOT_BOTTOM).append("\" stroke=\"").append(hex(MUTED))
                    .append("\" stroke-dasharray=\"5 4\"/>\n<text class=\"muted\" x=\"")
                    .append(labelOnLeft(x) ? x - 6 : x + 6).append("\" y=\"").append(PLOT_TOP + 14)
                    .append(labelOnLeft(x) ? "\" text-anchor=\"end" : "")
                    .append("\" font-size=\"12\">producers finished</text>\n");
        }
        for (int index = 0; index < series.size(); index++) {
            String color = hex(COLORS.get(index % COLORS.size()));
            for (int[][] segment : segments(seconds, series.get(index).values(), scale)) {
                out.append("<polyline fill=\"none\" stroke=\"").append(color)
                        .append("\" stroke-width=\"2\"").append(dashes(series.get(index))).append(" points=\"");
                for (int point = 0; point < segment[0].length; point++) {
                    out.append(segment[0][point]).append(',').append(segment[1][point]).append(' ');
                }
                out.append("\"/>\n");
            }
        }
        int legendX = PLOT_LEFT;
        int legendY = PLOT_BOTTOM + 72;
        for (int index = 0; index < series.size(); index++) {
            String name = series.get(index).name();
            // Approximate text width for the SVG legend, which has no font metrics.
            int width = 40 + (int) (name.length() * 7.5);
            if (legendX > PLOT_LEFT && legendX + width > PLOT_RIGHT) {
                legendX = PLOT_LEFT;
                legendY += LEGEND_ROW_HEIGHT;
            }
            out.append("<line x1=\"").append(legendX).append("\" y1=\"").append(legendY - 4)
                    .append("\" x2=\"").append(legendX + 18).append("\" y2=\"").append(legendY - 4)
                    .append("\" stroke=\"").append(hex(COLORS.get(index % COLORS.size())))
                    .append("\" stroke-width=\"2\"").append(dashes(series.get(index))).append("/>\n<text x=\"")
                    .append(legendX + 26).append("\" y=\"").append(legendY)
                    .append("\" font-size=\"14\">").append(xml(name)).append("</text>\n");
            legendX += width;
        }
        HdrHistogramRenderer.appendSvgFooter(out, footer, HEIGHT);
        return out.append("</svg>\n").toString();
    }

    // Round dots, two pixels wide and five apart
    private static BasicStroke stroke(Series series) {
        return series.dotted()
                ? new BasicStroke(2, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 10, new float[] {0.1f, 5}, 0)
                : new BasicStroke(2);
    }

    private static String dashes(Series series) {
        return series.dotted() ? " stroke-dasharray=\"0.1 5\" stroke-linecap=\"round\"" : "";
    }

    /** The runs of consecutive defined values of one series, as x and y pixel arrays. */
    private static List<int[][]> segments(double[] seconds, double[] values, Scale scale) {
        List<int[][]> segments = new ArrayList<>();
        int start = -1;
        for (int index = 0; index <= values.length; index++) {
            boolean defined = index < values.length && !Double.isNaN(values[index]);
            if (defined && start < 0) {
                start = index;
            } else if (!defined && start >= 0) {
                int length = index - start;
                int[] xs = new int[length];
                int[] ys = new int[length];
                for (int point = 0; point < length; point++) {
                    xs[point] = scale.x(seconds[start + point]);
                    ys[point] = scale.y(values[start + point]);
                }
                segments.add(new int[][] {xs, ys});
                start = -1;
            }
        }
        return segments;
    }

    private static List<Double> ticks(Scale scale) {
        double step = niceCeiling((scale.maxSeconds() - scale.minSeconds()) / 10);
        List<Double> ticks = new ArrayList<>();
        for (double tick = Math.ceil(scale.minSeconds() / step) * step; tick <= scale.maxSeconds(); tick += step) {
            ticks.add(tick);
        }
        return ticks;
    }

    /** The time axis step: the smallest of 1, 2 or 5 times a power of ten that is at least {@code value}. */
    static double niceCeiling(double value) {
        return ceiling(value, new double[] {1, 2, 5, 10});
    }

    /**
     * The top of the value axis, with finer steps than {@link #niceCeiling} so that the lines use most of the plot
     * height; five grid lines divide each of these into round values.
     */
    static double axisMaximum(double value) {
        return ceiling(value, new double[] {1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10});
    }

    private static double ceiling(double value, double[] factors) {
        if (value <= 0) {
            return 1;
        }
        double magnitude = Math.pow(10, Math.floor(Math.log10(value)));
        for (double factor : factors) {
            if (factor * magnitude >= value) {
                return factor * magnitude;
            }
        }
        return 10 * magnitude;
    }

    // The marker's label goes to the left of the line when the line is near the right edge of the plot.
    private static boolean labelOnLeft(int x) {
        return x > PLOT_RIGHT - 160;
    }

    static String compact(double value) {
        double magnitude = Math.abs(value);
        return magnitude >= 1_000_000 ? trim(value / 1_000_000) + "M"
                : magnitude >= 1_000 ? trim(value / 1_000) + "k" : trim(value);
    }

    private static String trim(double value) {
        String text = String.format(Locale.ROOT, "%.1f", value);
        return text.endsWith(".0") ? text.substring(0, text.length() - 2) : text;
    }

    private static String hex(Color color) {
        return String.format(Locale.ROOT, "#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }
}
