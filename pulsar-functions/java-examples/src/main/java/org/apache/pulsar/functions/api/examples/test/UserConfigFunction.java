/**
 * Copyright (c) 2018 Streamlio Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.functions.api.examples.test;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Optional;

public class UserConfigFunction implements Function<String, String> {

    @Override
    public String process(String input, Context context) {
        Optional<Object> whatToWrite = context.getUserConfigValue("WhatToWrite");
        if (whatToWrite.get() != null) {
            return (String)whatToWrite.get();
        } else {
            return "Not a nice way";
        }
    }
}

