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

import static org.apache.pulsar.admin.cli.utils.ValueValidationUtils.emptyCheck;
import static org.apache.pulsar.admin.cli.utils.ValueValidationUtils.maxValueCheck;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.util.RelativeTimeUtil;

public class TimeUnitToSecondsConverter extends BaseConverter<Long> {

    public TimeUnitToSecondsConverter(String optionName) {
        super(optionName);
    }

    @Override
    public Long convert(String str) {
        emptyCheck(getOptionName(), str);
        try {
            long inputValue = TimeUnit.SECONDS.toSeconds(
                    RelativeTimeUtil.parseRelativeTimeInSeconds(str.trim()));
            maxValueCheck(getOptionName(), inputValue, Long.MAX_VALUE);
            return inputValue;
        } catch (IllegalArgumentException exception) {
            throw new ParameterException("For input " + getOptionName() + ": " + exception.getMessage());
        }
    }
}
