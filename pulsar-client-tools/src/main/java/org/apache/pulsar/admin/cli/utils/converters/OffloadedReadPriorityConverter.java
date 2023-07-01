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
package org.apache.pulsar.admin.cli.utils.converters;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;

public class OffloadedReadPriorityConverter extends BaseConverter<OffloadedReadPriority> {

    public OffloadedReadPriorityConverter(String optionName) {
        super(optionName);
    }

    @Override
    public OffloadedReadPriority convert(String value) {
        try {
            return OffloadedReadPriority.fromString(value);
        } catch (Exception e) {
            throw new ParameterException("--offloadedReadPriority parameter must be one of "
                    + Arrays.stream(OffloadedReadPriority.values())
                    .map(OffloadedReadPriority::toString)
                    .collect(Collectors.joining(","))
                    + " but got: " + value, e);
        }
    }
}
