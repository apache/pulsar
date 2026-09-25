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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.heading.anchor.HeadingAnchorExtension;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.Image;
import org.commonmark.node.Link;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

/**
 * Renders a Markdown report to an HTML page beside it, {@code report.md} to {@code report.html}, with
 * commonmark-java, so that its links can be followed in a browser. Links to other Markdown documents are rewritten
 * to their HTML pages, and absolute file paths inside the run directory, as some tools write, to paths relative to
 * the page, so that a moved or archived run keeps working links. The stylesheet is inlined so that a page is a
 * single self-contained file.
 */
final class MarkdownPages {
    // Heading anchors give each heading a GitHub-style id, so in-page links such as #where-the-time-went work.
    private static final List<Extension> EXTENSIONS = List.of(TablesExtension.create(),
            HeadingAnchorExtension.create());
    private static final Parser PARSER = Parser.builder().extensions(EXTENSIONS).build();
    private static final HtmlRenderer RENDERER = HtmlRenderer.builder().extensions(EXTENSIONS).build();
    private static final String MARKDOWN_SUFFIX = ".md";

    private MarkdownPages() {
    }

    /**
     * Renders {@code markdown} to the HTML page beside it.
     *
     * @param root the run directory: absolute paths inside it become relative links
     * @return the HTML page
     */
    static Path renderHtml(Path markdown, Path root, String title) throws IOException {
        Path page = htmlPage(markdown);
        Path pageDirectory = page.toAbsolutePath().normalize().getParent();
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Node document = PARSER.parse(Files.readString(markdown));
        document.accept(new AbstractVisitor() {
            @Override
            public void visit(Link link) {
                link.setDestination(rewrite(link.getDestination(), pageDirectory, normalizedRoot));
                visitChildren(link);
            }

            @Override
            public void visit(Image image) {
                image.setDestination(rewrite(image.getDestination(), pageDirectory, normalizedRoot));
                visitChildren(image);
            }
        });
        String html = resource("report-page.html")
                .replace("{{title}}", escape(title))
                .replace("{{style}}", resource("report.css"))
                .replace("{{body}}", RENDERER.render(document));
        Files.writeString(page, html);
        return page;
    }

    /** The HTML page of a Markdown file: {@code report.md} is {@code report.html}. */
    static Path htmlPage(Path markdown) {
        String name = markdown.getFileName().toString();
        String base = name.endsWith(MARKDOWN_SUFFIX) ? name.substring(0, name.length() - MARKDOWN_SUFFIX.length())
                : name;
        return markdown.resolveSibling(base + ".html");
    }

    static String rewrite(String destination, Path pageDirectory, Path root) {
        if (destination == null || destination.isEmpty() || destination.startsWith("#")
                || destination.matches("^[a-zA-Z][a-zA-Z0-9+.-]*:.*")) {
            // An anchor on the same page, or a URL such as https: or mailto:
            return destination;
        }
        int fragmentStart = destination.indexOf('#');
        String path = fragmentStart < 0 ? destination : destination.substring(0, fragmentStart);
        String fragment = fragmentStart < 0 ? "" : destination.substring(fragmentStart);
        if (path.startsWith("/")) {
            try {
                Path absolute = Path.of(path).normalize();
                if (absolute.startsWith(root)) {
                    path = pageDirectory.relativize(absolute).toString();
                }
            } catch (InvalidPathException e) {
                // Not a file path; leave it as written
            }
        }
        if (path.endsWith(MARKDOWN_SUFFIX)) {
            path = path.substring(0, path.length() - MARKDOWN_SUFFIX.length()) + ".html";
        }
        return path + fragment;
    }

    private static String resource(String name) throws IOException {
        try (InputStream stream = MarkdownPages.class.getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("Missing launcher resource " + name);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
