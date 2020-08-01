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
package org.apache.pulsar.log4j2.appender.builder;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.CustomLevelConfig;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import org.apache.logging.log4j.core.layout.GelfLayout;
import org.apache.logging.log4j.core.util.Constants;
import org.apache.pulsar.log4j2.appender.PulsarAppender;
import org.testng.annotations.Test;

public class ConfigurationAssemblerTest {

    @Test
    public void testBuildConfiguration() {
        try {
            System.setProperty(Constants.LOG4J_CONTEXT_SELECTOR,
                    "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
            final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory
                    .newConfigurationBuilder();
            CustomConfigurationFactory.addTestFixtures("config name", builder);
            final Configuration configuration = builder.build();
            try (LoggerContext ctx = Configurator.initialize(configuration)) {
                validate(configuration);
            }
        } finally {
            System.getProperties().remove(Constants.LOG4J_CONTEXT_SELECTOR);
        }
    }

    @Test
    public void testCustomConfigurationFactory() {
        try {
            System.setProperty(ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
                    "org.apache.pulsar.log4j2.appender.builder.CustomConfigurationFactory");
            System.setProperty(Constants.LOG4J_CONTEXT_SELECTOR,
                    "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
            final Configuration config = ((LoggerContext) LogManager.getContext(false)).getConfiguration();
            validate(config);
        } finally {
            System.getProperties().remove(Constants.LOG4J_CONTEXT_SELECTOR);
            System.getProperties().remove(ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY);
        }
    }

    private void validate(final Configuration config) {
        assertNotNull(config);
        assertNotNull(config.getName());
        assertFalse(config.getName().isEmpty());
        assertNotNull(config, "No configuration created");
        assertEquals("Incorrect State: " + config.getState(), config.getState(), LifeCycle.State.STARTED);
        final Map<String, Appender> appenders = config.getAppenders();
        assertNotNull(appenders);
        assertEquals("Incorrect number of Appenders: " + appenders.size(), appenders.size(), 2);
        final PulsarAppender pulsarAppender = (PulsarAppender) appenders.get("Pulsar");
        final GelfLayout gelfLayout = (GelfLayout) pulsarAppender.getLayout();
        final Map<String, LoggerConfig> loggers = config.getLoggers();
        assertNotNull(loggers);
        assertEquals("Incorrect number of LoggerConfigs: " + loggers.size(), loggers.size(), 2);
        final LoggerConfig rootLoggerConfig = loggers.get("");
        assertEquals(Level.ERROR, rootLoggerConfig.getLevel());
        assertFalse(rootLoggerConfig.isIncludeLocation());
        final LoggerConfig loggerConfig = loggers.get("org.apache.logging.log4j");
        assertEquals(Level.DEBUG, loggerConfig.getLevel());
        assertTrue(loggerConfig.isIncludeLocation());
        final Filter filter = config.getFilter();
        assertNotNull(filter, "No Filter");
        assertTrue("Not a Threshold Filter", filter instanceof ThresholdFilter);
        final List<CustomLevelConfig> customLevels = config.getCustomLevels();
        assertNotNull(filter, "No CustomLevels");
        assertEquals(1, customLevels.size());
        final CustomLevelConfig customLevel = customLevels.get(0);
        assertEquals("Panic", customLevel.getLevelName());
        assertEquals(17, customLevel.getIntLevel());
        final Logger logger = LogManager.getLogger(getClass());
        logger.info("Welcome to Log4j!");
    }
}
