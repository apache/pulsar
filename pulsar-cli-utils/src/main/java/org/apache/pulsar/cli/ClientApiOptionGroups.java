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
package org.apache.pulsar.cli;

import picocli.CommandLine.Model.ArgGroupSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

/**
 * Support for commands whose options are split into picocli {@code @ArgGroup} sections by the client
 * API they apply to.
 *
 * <p>A command declares its client-specific options in {@code @ArgGroup(exclusive = false,
 * validate = false, heading = ...)} classes that implement {@link V4ClientOptions} or
 * {@link V5ClientOptions}; options in any other group, or in no group, apply to both clients. The
 * groups give {@code --help} one section per client, and {@link #validate(CommandSpec, ClientApi)}
 * rejects an option typed on the command line that belongs to the client not in use.
 */
public final class ClientApiOptionGroups {

    /** Heading of the section holding the options that apply to both clients. */
    public static final String COMMON_HEADING = "%nCommon options:%n";

    /** Heading of the section holding the v4-client options. */
    public static final String V4_HEADING =
            "%nv4 client options (default for persistent://, non-persistent:// and unprefixed topics):%n";

    /** Heading of the section holding the V5-client options. */
    public static final String V5_HEADING = "%nV5 client options (default for topic:// scalable topics):%n";

    /** Marker for an {@code @ArgGroup} class whose options only apply to the v4 client. */
    public interface V4ClientOptions {
    }

    /** Marker for an {@code @ArgGroup} class whose options only apply to the V5 client. */
    public interface V5ClientOptions {
    }

    private ClientApiOptionGroups() {
    }

    /**
     * Returns the client API an option is restricted to, or {@code null} if it applies to both.
     */
    public static ClientApi clientApiOf(OptionSpec option) {
        for (ArgGroupSpec group = option.group(); group != null; group = group.parentGroup()) {
            Class<?> type = group.typeInfo().getType();
            if (V4ClientOptions.class.isAssignableFrom(type)) {
                return ClientApi.V4;
            }
            if (V5ClientOptions.class.isAssignableFrom(type)) {
                return ClientApi.V5;
            }
        }
        return null;
    }

    /**
     * Rejects any option given on the command line that only applies to the other client API.
     *
     * <p>Only options typed on the command line are checked; values that come from a default value
     * provider (such as a configuration file) are not.
     *
     * @throws ParameterException naming the first such option
     */
    public static void validate(CommandSpec spec, ClientApi clientApi) {
        ParseResult parseResult = spec.commandLine().getParseResult();
        if (parseResult == null) {
            return;
        }
        for (OptionSpec option : parseResult.matchedOptionsSet()) {
            ClientApi optionApi = clientApiOf(option);
            if (optionApi != null && optionApi != clientApi) {
                throw new ParameterException(spec.commandLine(), String.format(
                        "%s applies only to the %s (the default for %s, or %s %s), but this invocation uses "
                                + "the %s.",
                        option.longestName(), optionApi.displayName(), optionApi.defaultTopics(),
                        ClientApi.OPTION_NAME, optionApi.name(), clientApi.displayName()));
            }
        }
    }

    /**
     * Returns the plain-text heading of the help section an option is listed under, or {@code null}
     * if it is not in a group with a heading.
     */
    public static String sectionHeading(OptionSpec option) {
        for (ArgGroupSpec group = option.group(); group != null; group = group.parentGroup()) {
            if (group.heading() != null) {
                return String.format(group.heading()).trim();
            }
        }
        return null;
    }
}
