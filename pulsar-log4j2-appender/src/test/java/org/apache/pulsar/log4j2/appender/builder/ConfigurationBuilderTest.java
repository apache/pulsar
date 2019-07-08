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

import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.testng.annotations.Test;

public class ConfigurationBuilderTest {

    private static final String INDENT = "  ";
    private static final String EOL = System.lineSeparator();

    private void addTestFixtures(final String name, final ConfigurationBuilder<BuiltConfiguration> builder) {
        builder.setConfigurationName(name);
        builder.setStatusLevel(Level.ERROR);
        builder.setShutdownTimeout(5000, TimeUnit.MILLISECONDS);
        builder.add(builder.newScriptFile("target/test-classes/scripts/filter.groovy").addIsWatched(true));
        builder.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.NEUTRAL)
                .addAttribute("level", Level.DEBUG));

        final AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "CONSOLE").addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        appenderBuilder.add(builder.newLayout("PatternLayout").
                addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable"));
        appenderBuilder.add(builder.newFilter("MarkerFilter", Filter.Result.DENY,
                Filter.Result.NEUTRAL).addAttribute("marker", "FLOW"));
        builder.add(appenderBuilder);

        final AppenderComponentBuilder appenderBuilder2 = builder.newAppender("Pulsar", "Pulsar")
            .addAttribute("serviceUrl", "pulsar://localhost:6650")
            .addAttribute("topic", "my-topic");
        appenderBuilder2.add(builder.newLayout("GelfLayout").
            addAttribute("host", "my-host").
            addComponent(builder.newKeyValuePair("extraField", "extraValue")));
        builder.add(appenderBuilder2);

        builder.add(builder.newLogger("org.apache.logging.log4j", Level.DEBUG, true).
                    add(builder.newAppenderRef("Stdout")).
                    addAttribute("additivity", false));
        builder.add(builder.newLogger("org.apache.logging.log4j.core", Level.DEBUG).
                    add(builder.newAppenderRef("Stdout")));
        builder.add(builder.newRootLogger(Level.ERROR).add(builder.newAppenderRef("Stdout")));

        builder.addProperty("MyKey", "MyValue");
        builder.add(builder.newCustomLevel("Panic", 17));
        builder.setPackages("foo,bar");
    }

    private final static String expectedXml =
            "<?xml version=\"1.0\" ?>" + EOL +
            "<Configuration name=\"config name\" status=\"ERROR\" packages=\"foo,bar\" shutdownTimeout=\"5000\">" + EOL +
                INDENT + "<Properties>" + EOL +
                INDENT + INDENT + "<Property name=\"MyKey\">MyValue</Property>" + EOL +
                INDENT + "</Properties>" + EOL +
                INDENT + "<Scripts>" + EOL +
                INDENT + INDENT + "<ScriptFile name=\"target/test-classes/scripts/filter.groovy\" path=\"target/test-classes/scripts/filter.groovy\" isWatched=\"true\"/>" + EOL +
                INDENT + "</Scripts>" + EOL +
                INDENT + "<CustomLevels>" + EOL +
                INDENT + INDENT + "<CustomLevel name=\"Panic\" intLevel=\"17\"/>" + EOL +
                INDENT + "</CustomLevels>" + EOL +
                INDENT + "<ThresholdFilter onMatch=\"ACCEPT\" onMisMatch=\"NEUTRAL\" level=\"DEBUG\"/>" + EOL +
                INDENT + "<Appenders>" + EOL +
                INDENT + INDENT + "<CONSOLE name=\"Stdout\" target=\"SYSTEM_OUT\">" + EOL +
                INDENT + INDENT + INDENT + "<PatternLayout pattern=\"%d [%t] %-5level: %msg%n%throwable\"/>" + EOL +
                INDENT + INDENT + INDENT + "<MarkerFilter onMatch=\"DENY\" onMisMatch=\"NEUTRAL\" marker=\"FLOW\"/>" + EOL +
                INDENT + INDENT + "</CONSOLE>" + EOL +
                INDENT + INDENT + "<Pulsar name=\"Pulsar\" serviceUrl=\"pulsar://localhost:6650\" topic=\"my-topic\">" + EOL +
                INDENT + INDENT + INDENT + "<GelfLayout host=\"my-host\">" + EOL +
                INDENT + INDENT + INDENT + INDENT + "<KeyValuePair key=\"extraField\" value=\"extraValue\"/>" + EOL +
                INDENT + INDENT + INDENT + "</GelfLayout>" + EOL +
                INDENT + INDENT + "</Pulsar>" + EOL +
                INDENT + "</Appenders>" + EOL +
                INDENT + "<Loggers>" + EOL +
                INDENT + INDENT + "<Logger name=\"org.apache.logging.log4j\" level=\"DEBUG\" includeLocation=\"true\" additivity=\"false\">" + EOL +
                INDENT + INDENT + INDENT + "<AppenderRef ref=\"Stdout\"/>" + EOL +
                INDENT + INDENT + "</Logger>" + EOL +
                INDENT + INDENT + "<Logger name=\"org.apache.logging.log4j.core\" level=\"DEBUG\">" + EOL +
                INDENT + INDENT + INDENT + "<AppenderRef ref=\"Stdout\"/>" + EOL +
                INDENT + INDENT + "</Logger>" + EOL +
                INDENT + INDENT + "<Root level=\"ERROR\">" + EOL +
                INDENT + INDENT + INDENT + "<AppenderRef ref=\"Stdout\"/>" + EOL +
                INDENT + INDENT + "</Root>" + EOL +
                INDENT + "</Loggers>" + EOL +
            "</Configuration>" + EOL;

    // TODO make test run properly on Windows
    @Test
    public void testXmlConstructing() {
        //assumeTrue(System.lineSeparator().length() == 1); // Only run test on platforms with single character line endings (such as Linux), not on Windows
        final ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        addTestFixtures("config name", builder);
        final String xmlConfiguration = builder.toXmlConfiguration();
        assertEquals(expectedXml, xmlConfiguration);
    }

}
