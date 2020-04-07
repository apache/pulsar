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
package org.apache.pulsar.client.util;

public class RetryMessageUtil {

    public final static String SYSTEM_PROPERTY_RECONSUMETIMES = "RECONSUMETIMES";
    public final static String SYSTEM_PROPERTY_DELAY_TIME = "DELAY_TIME";
    public final static String SYSTEM_PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    public final static String SYSTEM_PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    public final static String SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_IDY_TIME";
    
    public final static int MAX_RECONSUMETIMES = 16;
    public final static String RETRY_GROUP_TOPIC_SUFFIX = "-RETRY";
    public final static String DLQ_GROUP_TOPIC_SUFFIX = "-DLQ";
}
