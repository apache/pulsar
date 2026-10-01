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

import static org.apache.pulsar.tests.performance.report.ChartStyle.GRID;
import static org.apache.pulsar.tests.performance.report.ChartStyle.INK;
import static org.apache.pulsar.tests.performance.report.ChartStyle.MUTED;
import static org.apache.pulsar.tests.performance.report.ChartStyle.WIDTH;
import static org.apache.pulsar.tests.performance.report.ChartStyle.xml;
import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Draws a chart that compares a baseline run (A) with a comparison run (B), as SVG, in the width and style of the run
 * report's charts. The same chart is drawn in two forms:
 * <ul>
 *   <li>combined: both runs' lines in one diagram, A in {@link #A_COLOR} and B in {@link #B_COLOR};</li>
 *   <li>separate: A above B, in two panels with the same axes, each tinted in its run's color and labeled with it,
 *   so that the runs can be told apart at a glance and their values compared by position.</li>
 * </ul>
 * The lines are thin in both forms: the colors tell the runs apart.
 * Each run can have several lines, such as the published and dispatched rates; within a run they differ by dash.
 */
final class ComparisonRenderer {
    /** The baseline run's color: blue. */
    static final Color A_COLOR = new Color(52, 101, 164);
    /** The comparison run's color: orange, which stays distinct from the blue for color-blind readers. */
    static final Color B_COLOR = new Color(214, 95, 0);
    /** The separate charts' panel backgrounds: light tints of the runs' colors. */
    static final Color A_TINT = new Color(236, 242, 250);
    static final Color B_TINT = new Color(253, 241, 230);
    /** Every line's width, for both runs. */
    static final double STROKE = 1.5;
    private static final int PLOT_LEFT = 110;
    private static final int PLOT_RIGHT = WIDTH - 50;
    private static final int TITLE_HEIGHT = 110;
    private static final int COMBINED_PLOT_HEIGHT = 340;
    private static final int PANEL_PLOT_HEIGHT = 230;
    // Below each plot: the tick labels and the x axis title
    private static final int X_AXIS_HEIGHT = 56;
    // Above each panel of a separate chart: its run's badge
    private static final int BADGE_HEIGHT = 26;
    private static final int PANEL_GAP = BADGE_HEIGHT + 20;
    private static final int LEGEND_ROW_HEIGHT = 24;
    private static final int FOOTER_HEIGHT = 30;
    private static final int GRID_LINES = 5;
    private static final String PUBLISHED_DASHES = "9 6";

    /** The run a line belongs to. */
    enum Side {
        A, B
    }

    /**
     * One line; a {@link Double#NaN} value leaves a gap.
     *
     * @param dashed whether the line is dashed, which tells a run's lines apart, such as published from dispatched
     */
    record Line(String name, Side side, double[] x, double[] y, boolean dashed) {
    }

    /**
     * The x axis: seconds since the measurement start, or percentiles spread as
     * {@link HdrHistogramRenderer#percentileAxisPosition(double)} does, on a logarithmic scale.
     */
    record XAxis(String title, boolean percentile, double min, double max) {
        static XAxis seconds(double min, double max) {
            return new XAxis("Seconds since the measurement start", false, min, max);
        }

        static XAxis percentiles(double maxPosition) {
            return new XAxis("Percentile", true, 1, maxPosition);
        }
    }

    /**
     * What a chart shows, for both runs.
     *
     * @param yMax the top of the value axis, the same for both runs
     * @param logY whether the value axis is logarithmic, from {@code yMinLog} to {@code yMax}
     * @param finishedA the x position where A's producers finished, {@link Double#NaN} for no marker
     */
    record Chart(String title, String yLabel, XAxis x, double yMax, boolean logY, double yMinLog, List<Line> lines,
                 double finishedA, double finishedB) {
    }

    private record Panel(int top, int height) {
        int bottom() {
            return top + height;
        }
    }

    private ComparisonRenderer() {
    }

    /** The combined form: both runs' lines in one diagram. */
    static String combined(Chart chart, String labelA, String labelB, String footer) {
        Panel panel = new Panel(TITLE_HEIGHT, COMBINED_PLOT_HEIGHT);
        List<String> legend = new ArrayList<>();
        int legendTop = panel.bottom() + X_AXIS_HEIGHT + 10;
        int legendRows = legend(legend, chart.lines(), labelA, labelB, true, legendTop);
        int height = legendTop + legendRows * LEGEND_ROW_HEIGHT + FOOTER_HEIGHT;
        StringBuilder out = start(chart, height, "A: " + labelA + " · B: " + labelB);
        plot(out, chart, panel, null, chart.lines());
        marker(out, chart, panel, Side.A, chart.finishedA(), true);
        marker(out, chart, panel, Side.B, chart.finishedB(), true);
        legend.forEach(out::append);
        return end(out, footer, height);
    }

    /** The separate form: A above B, with the same axes, each in a panel tinted in its run's color. */
    static String separate(Chart chart, String labelA, String labelB, String footer) {
        Panel panelA = new Panel(TITLE_HEIGHT + BADGE_HEIGHT + 8, PANEL_PLOT_HEIGHT);
        Panel panelB = new Panel(panelA.bottom() + X_AXIS_HEIGHT + PANEL_GAP, PANEL_PLOT_HEIGHT);
        List<String> legend = new ArrayList<>();
        int legendTop = panelB.bottom() + X_AXIS_HEIGHT + 10;
        int legendRows = legend(legend, chart.lines(), labelA, labelB, false, legendTop);
        int height = legendTop + legendRows * LEGEND_ROW_HEIGHT + FOOTER_HEIGHT;
        StringBuilder out = start(chart, height, "A: " + labelA + " · B: " + labelB + " · both panels have the same"
                + " axes");
        plot(out, chart, panelA, Side.A, chart.lines().stream().filter(line -> line.side() == Side.A).toList());
        badge(out, panelA, Side.A, labelA);
        marker(out, chart, panelA, Side.A, chart.finishedA(), false);
        plot(out, chart, panelB, Side.B, chart.lines().stream().filter(line -> line.side() == Side.B).toList());
        badge(out, panelB, Side.B, labelB);
        marker(out, chart, panelB, Side.B, chart.finishedB(), false);
        legend.forEach(out::append);
        return end(out, footer, height);
    }

    private static StringBuilder start(Chart chart, int height, String subtitle) {
        StringBuilder out = new StringBuilder(32_000);
        out.append("<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ").append(WIDTH).append(' ').append(height)
                .append("\">\n<rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n")
                .append("<style>text{font-family:DejaVu Sans,Arial,sans-serif;fill:").append(hex(INK))
                .append("}.muted{fill:").append(hex(MUTED)).append("}</style>\n")
                .append("<text x=\"70\" y=\"55\" font-size=\"28\" font-weight=\"bold\">").append(xml(chart.title()))
                .append("</text>\n<text class=\"muted\" x=\"70\" y=\"82\" font-size=\"15\">")
                .append(xml(chart.yLabel() + " · " + subtitle)).append("</text>\n");
        return out;
    }

    private static String end(StringBuilder out, String footer, int height) {
        ChartStyle.appendSvgFooter(out, footer, height);
        return out.append("</svg>\n").toString();
    }

    // The grid, the axes and the lines of one plot; a panel of a separate chart gets its run's tint
    private static void plot(StringBuilder out, Chart chart, Panel panel, Side tint, List<Line> lines) {
        if (tint != null) {
            rect(out, PLOT_LEFT, panel.top(), PLOT_RIGHT - PLOT_LEFT, panel.height(),
                    tint == Side.A ? A_TINT : B_TINT);
        }
        for (double value : yTicks(chart)) {
            int y = y(chart, panel, value);
            out.append("<line x1=\"").append(PLOT_LEFT).append("\" y1=\"").append(y).append("\" x2=\"")
                    .append(PLOT_RIGHT).append("\" y2=\"").append(y).append("\" stroke=\"").append(hex(GRID))
                    .append("\"/>\n<text class=\"muted\" x=\"").append(PLOT_LEFT - 10).append("\" y=\"")
                    .append(y + 4).append("\" text-anchor=\"end\" font-size=\"12\">")
                    .append(TimeSeriesRenderer.compact(value)).append("</text>\n");
        }
        for (double tick : xTicks(chart.x())) {
            int x = x(chart.x(), tick);
            out.append("<line x1=\"").append(x).append("\" y1=\"").append(panel.bottom()).append("\" x2=\"")
                    .append(x).append("\" y2=\"").append(panel.bottom() + 5).append("\" stroke=\"")
                    .append(hex(INK)).append("\"/>\n<text x=\"").append(x).append("\" y=\"")
                    .append(panel.bottom() + 20).append("\" text-anchor=\"middle\" font-size=\"12\">")
                    .append(xml(xLabel(chart.x(), tick))).append("</text>\n");
        }
        out.append("<line x1=\"").append(PLOT_LEFT).append("\" y1=\"").append(panel.bottom()).append("\" x2=\"")
                .append(PLOT_RIGHT).append("\" y2=\"").append(panel.bottom()).append("\" stroke=\"").append(hex(INK))
                .append("\"/>\n<text x=\"").append((PLOT_LEFT + PLOT_RIGHT) / 2).append("\" y=\"")
                .append(panel.bottom() + 42).append("\" text-anchor=\"middle\" font-size=\"12\">")
                .append(xml(chart.x().title())).append("</text>\n");
        // A first, so that B's lines are drawn on top
        for (Side side : Side.values()) {
            for (Line line : lines) {
                if (line.side() == side) {
                    polylines(out, chart, panel, line);
                }
            }
        }
    }

    private static void polylines(StringBuilder out, Chart chart, Panel panel, Line line) {
        List<StringBuilder> segments = new ArrayList<>();
        StringBuilder current = null;
        for (int index = 0; index < line.x().length; index++) {
            double xValue = line.x()[index];
            double yValue = line.y()[index];
            boolean defined = !Double.isNaN(xValue) && !Double.isNaN(yValue) && xValue >= chart.x().min()
                    && xValue <= chart.x().max() && (!chart.logY() || yValue > 0);
            if (!defined) {
                current = null;
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
                segments.add(current);
            }
            current.append(x(chart.x(), xValue)).append(',').append(y(chart, panel, yValue)).append(' ');
        }
        for (StringBuilder segment : segments) {
            out.append("<polyline fill=\"none\" stroke=\"").append(hex(color(line.side())))
                    .append("\" stroke-width=\"").append(STROKE).append("\" stroke-linejoin=\"round\"")
                    .append(line.dashed() ? " stroke-dasharray=\"" + PUBLISHED_DASHES + "\"" : "")
                    .append(" points=\"").append(segment).append("\"/>\n");
        }
    }

    // The run's name in its color above its panel, where it doesn't cover the lines, so that a panel says which run
    // it shows
    private static void badge(StringBuilder out, Panel panel, Side side, String label) {
        String text = side + ": " + label;
        int width = 24 + (int) (text.length() * 8.2);
        int top = panel.top() - BADGE_HEIGHT - 4;
        rect(out, PLOT_LEFT, top, width, BADGE_HEIGHT, color(side));
        out.append("<text x=\"").append(PLOT_LEFT + 12).append("\" y=\"").append(top + 18)
                .append("\" font-size=\"14\" font-weight=\"bold\" style=\"fill:white\">").append(xml(text))
                .append("</text>\n");
    }

    private static void marker(StringBuilder out, Chart chart, Panel panel, Side side, double at, boolean prefix) {
        if (Double.isNaN(at) || at < chart.x().min() || at > chart.x().max()) {
            return;
        }
        int x = x(chart.x(), at);
        String label = (prefix ? side + ": " : "") + "gateways finished";
        boolean left = x > PLOT_RIGHT - 200;
        // A's label at the top, B's below it, so that the labels of close markers don't overlap
        int labelY = panel.top() + (side == Side.A || !prefix ? 14 : 30);
        out.append("<line x1=\"").append(x).append("\" y1=\"").append(panel.top()).append("\" x2=\"").append(x)
                .append("\" y2=\"").append(panel.bottom()).append("\" stroke=\"").append(hex(color(side)))
                .append("\" stroke-width=\"1.5\" stroke-dasharray=\"4 4\"/>\n<text x=\"")
                .append(left ? x - 6 : x + 6).append("\" y=\"").append(labelY)
                .append(left ? "\" text-anchor=\"end" : "").append("\" font-size=\"12\" style=\"fill:")
                .append(hex(color(side))).append("\">").append(xml(label)).append("</text>\n");
    }

    /**
     * Appends the legend: a combined chart lists each run's lines with their run, a separate chart each run's lines
     * in the run's color.
     *
     * @return the number of rows
     */
    private static int legend(List<String> out, List<Line> lines, String labelA, String labelB, boolean combined,
                              int top) {
        int x = PLOT_LEFT;
        int row = 0;
        for (Side side : Side.values()) {
            for (Line line : lines) {
                if (line.side() != side) {
                    continue;
                }
                String text = (combined ? side + " (" + (side == Side.A ? labelA : labelB) + "): " : side + ": ")
                        + line.name();
                int width = 44 + (int) (text.length() * 7.5);
                if (x > PLOT_LEFT && x + width > PLOT_RIGHT) {
                    x = PLOT_LEFT;
                    row++;
                }
                int y = top + row * LEGEND_ROW_HEIGHT + 14;
                out.add("<line x1=\"" + x + "\" y1=\"" + (y - 5) + "\" x2=\"" + (x + 28) + "\" y2=\"" + (y - 5)
                        + "\" stroke=\"" + hex(color(side)) + "\" stroke-width=\"" + STROKE + "\""
                        + (line.dashed() ? " stroke-dasharray=\"" + PUBLISHED_DASHES + "\"" : "") + "/>\n<text x=\""
                        + (x + 36) + "\" y=\"" + y + "\" font-size=\"14\">" + xml(text) + "</text>\n");
                x += width;
            }
        }
        return row + 1;
    }

    static Color color(Side side) {
        return side == Side.A ? A_COLOR : B_COLOR;
    }

    static int x(XAxis axis, double value) {
        double fraction = axis.percentile()
                ? (Math.log10(value) - Math.log10(axis.min())) / (Math.log10(axis.max()) - Math.log10(axis.min()))
                : (value - axis.min()) / (axis.max() - axis.min());
        return PLOT_LEFT + (int) Math.round(fraction * (PLOT_RIGHT - PLOT_LEFT));
    }

    private static int y(Chart chart, Panel panel, double value) {
        double fraction;
        if (chart.logY()) {
            double clamped = Math.max(value, chart.yMinLog());
            fraction = (Math.log10(clamped) - Math.log10(chart.yMinLog()))
                    / (Math.log10(chart.yMax()) - Math.log10(chart.yMinLog()));
        } else {
            fraction = value / chart.yMax();
        }
        return panel.bottom() - (int) Math.round(fraction * panel.height());
    }

    // Linear: five steps from zero; logarithmic: one line per decade
    static List<Double> yTicks(Chart chart) {
        List<Double> ticks = new ArrayList<>();
        if (chart.logY()) {
            for (double value = chart.yMinLog(); value <= chart.yMax() * 1.0001; value *= 10) {
                ticks.add(value);
            }
        } else {
            for (int line = 0; line <= GRID_LINES; line++) {
                ticks.add(chart.yMax() * line / GRID_LINES);
            }
        }
        return ticks;
    }

    // Seconds: a round step for about ten ticks; percentiles: each nine
    static List<Double> xTicks(XAxis axis) {
        List<Double> ticks = new ArrayList<>();
        if (axis.percentile()) {
            for (double position = 1; position <= axis.max() * 1.0001; position *= 10) {
                ticks.add(position);
            }
        } else {
            double step = TimeSeriesRenderer.niceCeiling((axis.max() - axis.min()) / 10);
            for (double tick = Math.ceil(axis.min() / step) * step; tick <= axis.max() + 1e-9; tick += step) {
                ticks.add(tick);
            }
        }
        return ticks;
    }

    private static String xLabel(XAxis axis, double tick) {
        return axis.percentile() ? HdrHistogramRenderer.percentileAxisLabel(tick)
                : String.format(Locale.ROOT, "%.0f", tick);
    }

    private static void rect(StringBuilder out, int x, int y, int width, int height, Color fill) {
        out.append("<rect x=\"").append(x).append("\" y=\"").append(y).append("\" width=\"").append(width)
                .append("\" height=\"").append(height).append("\" fill=\"").append(hex(fill)).append("\"/>\n");
    }

    static String hex(Color color) {
        return String.format(Locale.ROOT, "#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
    }
}
