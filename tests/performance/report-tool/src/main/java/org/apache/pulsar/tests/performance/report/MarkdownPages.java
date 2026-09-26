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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.ext.heading.anchor.HeadingAnchorExtension;
import org.commonmark.node.AbstractVisitor;
import org.commonmark.node.Code;
import org.commonmark.node.HtmlBlock;
import org.commonmark.node.Image;
import org.commonmark.node.Link;
import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

/**
 * Renders a Markdown report to an HTML page beside it, {@code report.md} to {@code report.html} and a directory's
 * {@code README.md} to its {@code index.html}, with commonmark-java, so that its links can be followed in a browser.
 * Links to other Markdown documents are rewritten to their HTML pages, and absolute file paths inside the run
 * directory, as some tools write, to paths relative to the page, so that a moved or archived run keeps working links.
 * The stylesheet is inlined so that a page is a single self-contained file.
 */
public final class MarkdownPages {
    // Heading anchors give each heading a GitHub-style id, so in-page links such as #where-the-time-went work.
    private static final List<Extension> EXTENSIONS = List.of(TablesExtension.create(),
            HeadingAnchorExtension.create());
    private static final Parser PARSER = Parser.builder().extensions(EXTENSIONS).build();
    private static final HtmlRenderer RENDERER = HtmlRenderer.builder().extensions(EXTENSIONS).build();
    private static final String MARKDOWN_SUFFIX = ".md";
    private static final String README = "README.md";
    private static final String INDEX = "index.html";
    // A package: lower-case segments, each followed by a dot, before a class name. Not part of a longer name or path.
    private static final Pattern JAVA_PACKAGE = Pattern.compile("(?<![\\w$./\\\\-])(?:[a-z][a-z0-9_]*\\.)+(?=[A-Z])");

    private MarkdownPages() {
    }

    /**
     * Renders {@code markdown} to the HTML page beside it.
     *
     * @param root the run directory: absolute paths inside it become relative links
     * @return the HTML page
     */
    static Path renderHtml(Path markdown, Path root, String title) throws IOException {
        return renderHtml(markdown, root, title, false);
    }

    /**
     * Renders {@code markdown} to the HTML page beside it.
     *
     * @param root the run directory: absolute paths inside it become relative links
     * @param abbreviateJavaNames whether code spans show Java names with abbreviated packages, as the flame graphs
     *                            do, with the full name as their tooltip
     * @return the HTML page
     */
    static Path renderHtml(Path markdown, Path root, String title, boolean abbreviateJavaNames)
            throws IOException {
        Path page = htmlPage(markdown);
        Path pageDirectory = page.toAbsolutePath().normalize().getParent();
        Path normalizedRoot = root.toAbsolutePath().normalize();
        Node document = PARSER.parse(Files.readString(markdown));
        Map<Node, String> fullNames = new IdentityHashMap<>();
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

            @Override
            public void visit(Code code) {
                String abbreviated = abbreviateJavaNames ? abbreviateJavaNames(code.getLiteral()) : code.getLiteral();
                if (!abbreviated.equals(code.getLiteral())) {
                    fullNames.put(code, code.getLiteral());
                    code.setLiteral(abbreviated);
                }
            }

            @Override
            public void visit(HtmlBlock block) {
                // Such as a <details> summary naming a method, whose Markdown inside is not parsed
                if (abbreviateJavaNames) {
                    block.setLiteral(abbreviateJavaNames(block.getLiteral()));
                }
            }
        });
        HtmlRenderer renderer = fullNames.isEmpty() ? RENDERER : HtmlRenderer.builder().extensions(EXTENSIONS)
                .attributeProviderFactory(context -> (node, tagName, attributes) -> {
                    String fullName = fullNames.get(node);
                    if (fullName != null) {
                        attributes.put("title", fullName);
                    }
                }).build();
        String html = resource("report-page.html")
                .replace("{{title}}", escape(title))
                .replace("{{style}}", resource("report.css"))
                .replace("{{body}}", renderer.render(document));
        Files.writeString(page, html);
        return page;
    }

    /**
     * The HTML page of a Markdown file: {@code report.md} is {@code report.html}, and a directory's {@code README.md}
     * is its {@code index.html}, which HTTP servers serve for the directory.
     */
    public static Path htmlPage(Path markdown) {
        return markdown.resolveSibling(htmlFileName(markdown.getFileName().toString()));
    }

    private static String htmlFileName(String markdownFileName) {
        if (markdownFileName.equals(README)) {
            return INDEX;
        }
        String base = markdownFileName.endsWith(MARKDOWN_SUFFIX)
                ? markdownFileName.substring(0, markdownFileName.length() - MARKDOWN_SUFFIX.length())
                : markdownFileName;
        return base + ".html";
    }

    /**
     * Abbreviates the packages of the Java names in {@code text} to their initials, as {@code stacks
     * --package-names abbreviate} does: {@code org.apache.pulsar.broker.service.Consumer.sendMessages} is
     * {@code o.a.p.b.s.Consumer.sendMessages}. Native names such as {@code libjvm.so.Unsafe_Park}, file paths and
     * regular expressions stay as they are.
     */
    static String abbreviateJavaNames(String text) {
        Matcher matcher = JAVA_PACKAGE.matcher(text);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String packageName = matcher.group();
            String replacement = packageName.contains(".so.") || packageName.startsWith("so.") ? packageName
                    : Arrays.stream(packageName.split("\\.")).map(segment -> segment.substring(0, 1))
                            .collect(Collectors.joining(".", "", "."));
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        return matcher.appendTail(result).toString();
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
            // A directory's README.md has its page as the directory's index.html. The link names the page rather than
            // the directory, so that it also works when the pages are opened from the file system.
            int fileNameStart = path.lastIndexOf('/') + 1;
            path = path.substring(0, fileNameStart) + htmlFileName(path.substring(fileNameStart));
        }
        return path + fragment;
    }

    private static String resource(String name) throws IOException {
        try (InputStream stream = MarkdownPages.class.getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("Missing report tool resource " + name);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
