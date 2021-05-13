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
package org.apache.pulsar.sql.presto;

import io.prestosql.decoder.FieldValueProvider;

/**
 * custom FieldValueProvider for Pulsar.
 */
public class PulsarFieldValueProviders {

    public static FieldValueProvider doubleValueProvider(double value) {
        return new FieldValueProvider() {
            @Override
            public double getDouble() {
                return value;
            }

            @Override
            public boolean isNull() {
                return false;
            }
        };
    }

    /**
     * FieldValueProvider for Time (Data,Timstamp etc.) with indicate Null instead of longValueProvider.
     * @param value
     * @param isNull
     * @return
     */
    public static FieldValueProvider timeValueProvider(long value, boolean isNull) {
        return new FieldValueProvider() {
            @Override
                public long getLong() {
                    return value;
                }

            @Override
            public boolean isNull() {
                return isNull;
            }
        };
    }

}
