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
import java.util.List;

/**
 * The look the report's charts share, so that they line up in a report: their width, colors, and the footer that
 * says which run a chart shows.
 */
final class ChartStyle {
    /** Every chart's width, so that the charts in a report are as wide as each other. */
    static final int WIDTH = 1400;
    static final Color INK = new Color(20, 43, 64);
    static final Color MUTED = new Color(80, 98, 117);
    static final Color GRID = new Color(218, 225, 232);
    static final Color PRODUCER = new Color(0, 123, 155);
    static final Color CONSUMER = new Color(189, 91, 36);
    /** The colors of a chart's lines, in order: the producers first, then each consumer application. */
    static final List<Color> SERIES_COLORS = List.of(PRODUCER, CONSUMER, new Color(96, 70, 160),
            new Color(46, 125, 50), new Color(173, 20, 87), new Color(120, 144, 156));
    /**
     * The producers' line: round dots, two pixels wide and five apart, so that the consumer lines it often overlaps
     * stay visible.
     */
    static final BasicStroke DOTTED =
            new BasicStroke(2, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 10, new float[] {0.1f, 5}, 0);
    // The footer that says which run a chart shows: small, as it is read only when needed
    private static final int FOOTER_FONT_SIZE = 10;
    private static final int FOOTER_MARGIN = 12;

    private ChartStyle() {
    }

    /** The color of a chart's {@code index}th line. */
    static Color seriesColor(int index) {
        return SERIES_COLORS.get(index % SERIES_COLORS.size());
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

    /**
     * Removes the width and height of an SVG's root element and keeps its viewBox, so that the chart scales to the
     * space it gets with its proportions kept: the browser window when the chart is opened on its own, or the page
     * width in a report.
     */
    static String scalable(String svg) {
        int start = svg.indexOf("<svg");
        int end = start < 0 ? -1 : svg.indexOf('>', start);
        if (end < 0) {
            return svg;
        }
        String root = svg.substring(start, end).replaceAll("\\s(width|height)=\"[^\"]*\"", "");
        return svg.substring(0, start) + root + svg.substring(end);
    }

    static String xml(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
