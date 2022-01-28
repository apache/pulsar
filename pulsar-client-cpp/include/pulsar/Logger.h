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
#pragma once

#include <memory>
#include <string>
#include <pulsar/defines.h>

namespace pulsar {

class PULSAR_PUBLIC Logger {
   public:
    enum Level
    {
        LEVEL_DEBUG = 0,
        LEVEL_INFO = 1,
        LEVEL_WARN = 2,
        LEVEL_ERROR = 3
    };

    virtual ~Logger() {}

    /**
     * Check whether the log level is enabled
     *
     * @param level the Logger::Level
     * @return true if log is enabled
     */
    virtual bool isEnabled(Level level) = 0;

    /**
     * Log the message with related metadata
     *
     * @param level the Logger::Level
     * @param line the line number of this log
     * @param message the message to log
     */
    virtual void log(Level level, int line, const std::string& message) = 0;
};

class PULSAR_PUBLIC LoggerFactory {
   public:
    virtual ~LoggerFactory() {}

    /**
     * Create a Logger that is created from the filename
     *
     * @param fileName the filename that is used to construct the Logger
     * @return a pointer to the created Logger instance
     * @note the pointer must be allocated with the `new` keyword in C++
     */
    virtual Logger* getLogger(const std::string& fileName) = 0;
};

}  // namespace pulsar
