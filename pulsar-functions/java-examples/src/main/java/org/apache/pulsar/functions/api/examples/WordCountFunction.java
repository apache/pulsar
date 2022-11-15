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
package org.apache.pulsar.functions.api.examples;

import java.util.Arrays;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

/**
 * The classic word count example done using pulsar functions
 * Each input message is a sentence that split into words and each word counted.
 * The built in counter state is used to keep track of the word count in a
 * persistent and consistent manner.
 */
public class WordCountFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        Arrays.asList(input.split("\\s+")).forEach(word -> context.incrCounter(word, 1));
        return null;
    }
}
