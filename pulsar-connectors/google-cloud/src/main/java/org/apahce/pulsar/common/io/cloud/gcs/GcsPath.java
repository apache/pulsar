/**
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
package org.apahce.pulsar.common.io.cloud.gcs;

import com.google.api.services.storage.model.StorageObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.api.client.util.Strings.isNullOrEmpty;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Copied from apache beam project
 *
 * Implements the Java NIO {@link Path} API for Google Cloud Storage paths.
 *
 * <p>GcsPath uses a slash ('/') as a directory separator.  Below is
 * a summary of how slashes are treated:
 * <ul>
 *   <li> A GCS bucket may not contain a slash.  An object may contain zero or
 *        more slashes.
 *   <li> A trailing slash always indicates a directory, which is compliant
 *        with POSIX.1-2008.
 *   <li> Slashes separate components of a path.  Empty components are allowed,
 *        these are represented as repeated slashes.  An empty component always
 *        refers to a directory, and always ends in a slash.
 *   <li> {@link #getParent()}} always returns a path ending in a slash, as the
 *        parent of a GcsPath is always a directory.
 *   <li> Use {@link #resolve(String)} to append elements to a GcsPath -- this
 *        applies the rules consistently and is highly recommended over any
 *        custom string concatenation.
 * </ul>
 *
 * <p>GcsPath treats all GCS objects and buckets as belonging to the same
 * filesystem, so the root of a GcsPath is the GcsPath bucket="", object="".
 *
 * <p>Relative paths are not associated with any bucket.  This matches common
 * treatment of Path in which relative paths can be constructed from one
 * filesystem and appended to another filesystem.
 *
 * @see <a href=
 * "http://docs.oracle.com/javase/tutorial/essential/io/pathOps.html"
 * >Java Tutorials: Path Operations</a>
 */
public class GcsPath implements Path, Serializable {

    public static final String SCHEME = "gs";

    /**
     * Creates a GcsPath from a URI.
     *
     * <p>The URI must be in the form {@code gs://[bucket]/[path]}, and may not
     * contain a port, user info, a query, or a fragment.
     */
    public static GcsPath fromUri(URI uri) {
        checkArgument(uri.getScheme().equalsIgnoreCase(SCHEME), "URI: %s is not a GCS URI", uri);
        checkArgument(uri.getPort() == -1,
                "GCS URI may not specify port: %s (%i)", uri, uri.getPort());
        checkArgument(
                isNullOrEmpty(uri.getUserInfo()),
                "GCS URI may not specify userInfo: %s (%s)", uri, uri.getUserInfo());
        checkArgument(
                isNullOrEmpty(uri.getQuery()),
                "GCS URI may not specify query: %s (%s)", uri, uri.getQuery());
        checkArgument(
                isNullOrEmpty(uri.getFragment()),
                "GCS URI may not specify fragment: %s (%s)", uri, uri.getFragment());

        return fromUri(uri.toString());
    }

    /**
     * Pattern that is used to parse a GCS URL.
     *
     * <p>This is used to separate the components.  Verification is handled
     * separately.
     */
    public static final Pattern GCS_URI =
            Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<OBJECT>.*))?");

    /**
     * Creates a GcsPath from a URI in string form.
     *
     * <p>This does not use URI parsing, which means it may accept patterns that
     * the URI parser would not accept.
     */
    public static GcsPath fromUri(String uri) {
        Matcher m = GCS_URI.matcher(uri);
        checkArgument(m.matches(), "Invalid GCS URI: %s", uri);

        checkArgument(m.group("SCHEME").equalsIgnoreCase(SCHEME),
                "URI: %s is not a GCS URI", uri);
        return new GcsPath(null, m.group("BUCKET"), m.group("OBJECT"));
    }

    /**
     * Pattern that is used to parse a GCS resource name.
     */
    private static final Pattern GCS_RESOURCE_NAME =
            Pattern.compile("storage.googleapis.com/(?<BUCKET>[^/]+)(/(?<OBJECT>.*))?");

    /**
     * Creates a GcsPath from a OnePlatform resource name in string form.
     */
    public static GcsPath fromResourceName(String name) {
        Matcher m = GCS_RESOURCE_NAME.matcher(name);
        checkArgument(m.matches(), "Invalid GCS resource name: %s", name);

        return new GcsPath(null, m.group("BUCKET"), m.group("OBJECT"));
    }

    /**
     * Creates a GcsPath from a {@linkplain StorageObject}.
     */
    public static GcsPath fromObject(StorageObject object) {
        return new GcsPath(null, object.getBucket(), object.getName());
    }

    /**
     * Creates a GcsPath from bucket and object components.
     *
     * <p>A GcsPath without a bucket name is treated as a relative path, which
     * is a path component with no linkage to the root element.  This is similar
     * to a Unix path that does not begin with the root marker (a slash).
     * GCS has different naming constraints and APIs for working with buckets and
     * objects, so these two concepts are kept separate to avoid accidental
     * attempts to treat objects as buckets, or vice versa, as much as possible.
     *
     * <p>A GcsPath without an object name is a bucket reference.
     * A bucket is always a directory, which could be used to lookup or add
     * files to a bucket, but could not be opened as a file.
     *
     * <p>A GcsPath containing neither bucket or object names is treated as
     * the root of the GCS filesystem.  A listing on the root element would return
     * the buckets available to the user.
     *
     * <p>If {@code null} is passed as either parameter, it is converted to an
     * empty string internally for consistency.  There is no distinction between
     * an empty string and a {@code null}, as neither are allowed by GCS.
     *
     * @param bucket a GCS bucket name, or none ({@code null} or an empty string)
     *               if the object is not associated with a bucket
     *               (e.g. relative paths or the root node).
     * @param object a GCS object path, or none ({@code null} or an empty string)
     *               for no object.
     */
    public static GcsPath fromComponents(@Nullable String bucket,
                                         @Nullable String object) {
        return new GcsPath(null, bucket, object);
    }

    @Nullable
    private transient FileSystem fs;
    @Nonnull
    private final String bucket;
    @Nonnull
    private final String object;

    /**
     * Constructs a GcsPath.
     *
     * @param fs the associated FileSystem, if any
     * @param bucket the associated bucket, or none ({@code null} or an empty
     *               string) for a relative path component
     * @param object the object, which is a fully-qualified object name if bucket
     *               was also provided, or none ({@code null} or an empty string)
     *               for no object
     * @throws java.lang.IllegalArgumentException if the bucket of object names
     *         are invalid.
     */
    public GcsPath(@Nullable FileSystem fs,
                   @Nullable String bucket,
                   @Nullable String object) {
        if (bucket == null) {
            bucket = "";
        }
        checkArgument(!bucket.contains("/"),
                "GCS bucket may not contain a slash");
        checkArgument(bucket.isEmpty()
                        || bucket.matches("[a-z0-9][-_a-z0-9.]+[a-z0-9]"),
                "GCS bucket names must contain only lowercase letters, numbers, "
                        + "dashes (-), underscores (_), and dots (.). Bucket names "
                        + "must start and end with a number or letter. "
                        + "See https://developers.google.com/storage/docs/bucketnaming "
                        + "for more details.  Bucket name: " + bucket);

        if (object == null) {
            object = "";
        }
        checkArgument(
                object.indexOf('\n') < 0 && object.indexOf('\r') < 0,
                "GCS object names must not contain Carriage Return or "
                        + "Line Feed characters.");

        this.fs = fs;
        this.bucket = bucket;
        this.object = object;
    }

    /**
     * Returns the bucket name associated with this GCS path, or an empty string
     * if this is a relative path component.
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * Returns the object name associated with this GCS path, or an empty string
     * if no object is specified.
     */
    public String getObject() {
        return object;
    }

    public void setFileSystem(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public FileSystem getFileSystem() {
        return fs;
    }

    // Absolute paths are those that have a bucket and the root path.
    @Override
    public boolean isAbsolute() {
        return !bucket.isEmpty() || object.isEmpty();
    }

    @Override
    public GcsPath getRoot() {
        return new GcsPath(fs, "", "");
    }

    @Override
    public GcsPath getFileName() {
        int nameCount = getNameCount();
        if (nameCount < 2) {
            throw new UnsupportedOperationException(
                    "Can't get filename from root path in the bucket: " + this);
        }
        return getName(nameCount - 1);
    }

    /**
     * Returns the <em>parent path</em>, or {@code null} if this path does not
     * have a parent.
     *
     * <p>Returns a path that ends in '/', as the parent path always refers to
     * a directory.
     */
    @Override
    public GcsPath getParent() {
        if (bucket.isEmpty() && object.isEmpty()) {
            // The root path has no parent, by definition.
            return null;
        }

        if (object.isEmpty()) {
            // A GCS bucket. All buckets come from a common root.
            return getRoot();
        }

        // Skip last character, in case it is a trailing slash.
        int i = object.lastIndexOf('/', object.length() - 2);
        if (i <= 0) {
            if (bucket.isEmpty()) {
                // Relative paths are not attached to the root node.
                return null;
            }
            return new GcsPath(fs, bucket, "");
        }

        // Retain trailing slash.
        return new GcsPath(fs, bucket, object.substring(0, i + 1));
    }

    @Override
    public int getNameCount() {
        int count = bucket.isEmpty() ? 0 : 1;
        if (object.isEmpty()) {
            return count;
        }

        // Add another for each separator found.
        int index = -1;
        while ((index = object.indexOf('/', index + 1)) != -1) {
            count++;
        }

        return object.endsWith("/") ? count : count + 1;
    }

    @Override
    public GcsPath getName(int count) {
        checkArgument(count >= 0);

        Iterator<Path> iterator = iterator();
        for (int i = 0; i < count; ++i) {
            checkArgument(iterator.hasNext());
            iterator.next();
        }

        checkArgument(iterator.hasNext());
        return (GcsPath) iterator.next();
    }

    @Override
    public GcsPath subpath(int beginIndex, int endIndex) {
        checkArgument(beginIndex >= 0);
        checkArgument(endIndex > beginIndex);

        Iterator<Path> iterator = iterator();
        for (int i = 0; i < beginIndex; ++i) {
            checkArgument(iterator.hasNext());
            iterator.next();
        }

        GcsPath path = null;
        while (beginIndex < endIndex) {
            checkArgument(iterator.hasNext());
            if (path == null) {
                path = (GcsPath) iterator.next();
            } else {
                path = path.resolve(iterator.next());
            }
            ++beginIndex;
        }

        return path;
    }

    @Override
    public boolean startsWith(Path other) {
        if (other instanceof GcsPath) {
            GcsPath gcsPath = (GcsPath) other;
            return startsWith(gcsPath.bucketAndObject());
        } else {
            return startsWith(other.toString());
        }
    }

    @Override
    public boolean startsWith(String prefix) {
        return bucketAndObject().startsWith(prefix);
    }

    @Override
    public boolean endsWith(Path other) {
        if (other instanceof GcsPath) {
            GcsPath gcsPath = (GcsPath) other;
            return endsWith(gcsPath.bucketAndObject());
        } else {
            return endsWith(other.toString());
        }
    }

    @Override
    public boolean endsWith(String suffix) {
        return bucketAndObject().endsWith(suffix);
    }

    // TODO: support "." and ".." path components?
    @Override
    public GcsPath normalize() {
        return this;
    }

    @Override
    public GcsPath resolve(Path other) {
        if (other instanceof GcsPath) {
            GcsPath path = (GcsPath) other;
            if (path.isAbsolute()) {
                return path;
            } else {
                return resolve(path.getObject());
            }
        } else {
            return resolve(other.toString());
        }
    }

    @Override
    public GcsPath resolve(String other) {
        if (bucket.isEmpty() && object.isEmpty()) {
            // Resolve on a root path is equivalent to looking up a bucket and object.
            other = SCHEME + "://" + other;
        }

        if (other.startsWith(SCHEME + "://")) {
            GcsPath path = GcsPath.fromUri(other);
            path.setFileSystem(getFileSystem());
            return path;
        }

        if (other.isEmpty()) {
            // An empty component MUST refer to a directory.
            other = "/";
        }

        if (object.isEmpty()) {
            return new GcsPath(fs, bucket, other);
        } else if (object.endsWith("/")) {
            return new GcsPath(fs, bucket, object + other);
        } else {
            return new GcsPath(fs, bucket, object + "/" + other);
        }
    }

    @Override
    public Path resolveSibling(Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path resolveSibling(String other) {
        if (getNameCount() < 2) {
            throw new UnsupportedOperationException("Can't resolve the sibling of a root path: " + this);
        }
        GcsPath parent = getParent();
        return (parent == null) ? fromUri(other) : parent.resolve(other);
    }

    @Override
    public Path relativize(Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GcsPath toAbsolutePath() {
        return this;
    }

    @Override
    public GcsPath toRealPath(LinkOption... options) throws IOException {
        return this;
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events,
                             WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        return new NameIterator(fs, !bucket.isEmpty(), bucketAndObject());
    }

    private static class NameIterator implements Iterator<Path> {
        private final FileSystem fs;
        private boolean fullPath;
        private String name;

        NameIterator(FileSystem fs, boolean fullPath, String name) {
            this.fs = fs;
            this.fullPath = fullPath;
            this.name = name;
        }

        @Override
        public boolean hasNext() {
            return !isNullOrEmpty(name);
        }

        @Override
        public GcsPath next() {
            int i = name.indexOf('/');
            String component;
            if (i >= 0) {
                component = name.substring(0, i);
                name = name.substring(i + 1);
            } else {
                component = name;
                name = null;
            }
            if (fullPath) {
                fullPath = false;
                return new GcsPath(fs, component, "");
            } else {
                // Relative paths have no bucket.
                return new GcsPath(fs, "", component);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int compareTo(Path other) {
        if (!(other instanceof GcsPath)) {
            throw new ClassCastException();
        }

        GcsPath path = (GcsPath) other;
        int b = bucket.compareTo(path.bucket);
        if (b != 0) {
            return b;
        }

        // Compare a component at a time, so that the separator char doesn't
        // get compared against component contents.  Eg, "a/b" < "a-1/b".
        Iterator<Path> left = iterator();
        Iterator<Path> right = path.iterator();

        while (left.hasNext() && right.hasNext()) {
            String leftStr = left.next().toString();
            String rightStr = right.next().toString();
            int c = leftStr.compareTo(rightStr);
            if (c != 0) {
                return c;
            }
        }

        if (!left.hasNext() && !right.hasNext()) {
            return 0;
        } else {
            return left.hasNext() ? 1 : -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GcsPath paths = (GcsPath) o;
        return bucket.equals(paths.bucket) && object.equals(paths.object);
    }

    @Override
    public int hashCode() {
        int result = bucket.hashCode();
        result = 31 * result + object.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (!isAbsolute()) {
            return object;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(SCHEME)
                .append("://");
        if (!bucket.isEmpty()) {
            sb.append(bucket)
                    .append('/');
        }
        sb.append(object);
        return sb.toString();
    }

    // TODO: Consider using resource names for all GCS paths used by the SDK.
    public String toResourceName() {
        StringBuilder sb = new StringBuilder();
        sb.append("storage.googleapis.com/");
        if (!bucket.isEmpty()) {
            sb.append(bucket).append('/');
        }
        sb.append(object);
        return sb.toString();
    }

    @Override
    public URI toUri() {
        try {
            return new URI(SCHEME, "//" + bucketAndObject(), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to create URI for GCS path " + this);
        }
    }

    private String bucketAndObject() {
        if (bucket.isEmpty()) {
            return object;
        } else {
            return bucket + "/" + object;
        }
    }
}

