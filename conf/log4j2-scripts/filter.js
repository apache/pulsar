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
// This is a sample file which can be used by log4j2 to: only log debug-statement which has text: pulsar-topic-name
var result = false;
var topicName = "pulsar-topic-name";
/*
 * Find more logEvent attributes at :
 * https://github.com/apache/logging-log4j2/blob/dbd2d252a1b4139a9bd9eb213c89f28498db6dcf/log4j-core/src/main/java/org/apache/logging/log4j/core/LogEvent.java
 */
if (logEvent.getLevel() == "DEBUG"){
        if(logEvent.getMessage().getFormattedMessage().indexOf(topicName)!=-1) {
                result = true;
        }
} else {
        result = true;
}
result;