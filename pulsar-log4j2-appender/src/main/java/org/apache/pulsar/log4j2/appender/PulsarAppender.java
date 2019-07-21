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
package org.apache.pulsar.log4j2.appender;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.AbstractLifeCycle;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.layout.SerializedLayout;

/**
 * The PulsarAppender logs events to an Apache Pulsar topic.
 *
 * <p>Each log event is sent as a Pulsar record.
 */
@Plugin(
    name = "Pulsar",
    category = Node.CATEGORY,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public final class PulsarAppender extends AbstractAppender {

    /**
     * Builds PulsarAppender instances.
     * @param <B> The type to build
     */
    public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<PulsarAppender> {

        @PluginAttribute("key")
        private String key;

        @PluginAttribute("topic")
        private String topic;

        @PluginAttribute("serviceUrl")
        private String serviceUrl;

        @PluginAttribute(value = "avoidRecursive", defaultBoolean = true)
        private boolean avoidRecursive;

        @PluginAttribute(value = "syncSend", defaultBoolean = false)
        private boolean syncSend;

        @PluginElement("Properties")
        private Property[] properties;

        @SuppressWarnings("resource")
        @Override
        public PulsarAppender build() {
            final Layout<? extends Serializable> layout = getLayout();
            if (layout == null) {
                AbstractLifeCycle.LOGGER.error("No layout provided for PulsarAppender");
                return null;
            }
            PulsarManager manager = new PulsarManager(
                getConfiguration().getLoggerContext(),
                getName(),
                serviceUrl,
                topic,
                syncSend,
                properties,
                key);
            return new PulsarAppender(
                getName(),
                layout,
                getFilter(),
                isIgnoreExceptions(),
                avoidRecursive,
                manager);
        }

        public String getTopic() {
            return topic;
        }

        public boolean isSyncSend() {
            return syncSend;
        }

        public Property[] getProperties() {
            return properties;
        }

        public B setTopic(final String topic) {
            this.topic = topic;
            return asBuilder();
        }

        public B setSyncSend(final boolean syncSend) {
            this.syncSend = syncSend;
            return asBuilder();
        }

        public B setProperties(final Property[] properties) {
            this.properties = properties;
            return asBuilder();
        }
    }

    /**
     * Creates a builder for a PulsarAppender.
     * @return a builder for a PulsarAppender.
     */
    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

    private final boolean avoidRecursive;
    private final PulsarManager manager;

    private PulsarAppender(
            final String name,
            final Layout<? extends Serializable> layout,
            final Filter filter,
            final boolean ignoreExceptions,
            final boolean avoidRecursive,
            final PulsarManager manager) {
        super(name, filter, layout, ignoreExceptions);
        this.avoidRecursive = avoidRecursive;
        this.manager = Objects.requireNonNull(manager, "manager");
    }

    @Override
    public void append(final LogEvent event) {
        if (avoidRecursive
            && event.getLoggerName() != null
            && event.getLoggerName().startsWith("org.apache.pulsar")) {
            LOGGER.warn("Recursive logging from [{}] for appender [{}].", event.getLoggerName(), getName());
        } else {
            try {
                tryAppend(event);
            } catch (final Exception e) {
                error("Unable to write to Pulsar in appender [" + getName() + "]", event, e);
            }
        }
    }

    private void tryAppend(final LogEvent event) {
        final Layout<? extends Serializable> layout = getLayout();
        byte[] data;
        if (layout instanceof SerializedLayout) {
            final byte[] header = layout.getHeader();
            final byte[] body = layout.toByteArray(event);
            data = new byte[header.length + body.length];
            System.arraycopy(header, 0, data, 0, header.length);
            System.arraycopy(body, 0, data, header.length, body.length);
        } else {
            data = layout.toByteArray(event);
        }
        manager.send(data);
    }

    @Override
    public void start() {
        super.start();
        try {
            manager.startup();
        } catch (Exception e) {
            // fail to start the manager
        }
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        setStopping();
        boolean stopped = super.stop(timeout, timeUnit, false);
        stopped &= manager.stop(timeout, timeUnit);
        setStopped();
        return stopped;
    }

    @Override
    public String toString() {
        return "PulsarAppender{" +
            "name=" + getName() +
            ", state=" + getState() +
            ", serviceUrl=" + manager.getServiceUrl() +
            ", topic=" + manager.getTopic() +
            '}';
    }

}
