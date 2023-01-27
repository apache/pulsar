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
package org.apache.pulsar.admin.cli.extensions;

import java.util.List;
import java.util.Map;

/**
 * Custom command.
 */
public interface CustomCommand {

    /**
     * Name of the command.
     * @return the name
     */
    String name();

    /**
     * Descritption of the command.
     * @return the description
     */
    String description();

    /**
     * The parameters for the command.
     * @return the parameters
     */
    List<ParameterDescriptor> parameters();

    /**
     * Execute the command.
     * @param parameters the parameters, one entry per each parameter name
     * @param context access the environment
     * @return false in case of failure
     * @throws Exception
     */
    boolean execute(Map<String, Object> parameters, CommandExecutionContext context) throws Exception;

}
