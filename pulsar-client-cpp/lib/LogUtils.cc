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
#include "LogUtils.h"

#include <iostream>

#include "SimpleLoggerImpl.h"
#include "Log4CxxLogger.h"

namespace pulsar {

void LogUtils::init(const std::string& logfilePath) {
// If this is called explicitely, we fallback to Log4cxx config, if enabled

#ifdef USE_LOG4CXX
    if (!logfilePath.empty()) {
        setLoggerFactory(Log4CxxLoggerFactory::create(logfilePath));
    } else {
        setLoggerFactory(Log4CxxLoggerFactory::create());
    }
#endif  // USE_LOG4CXX
}

static LoggerFactoryPtr s_loggerFactory;

void LogUtils::setLoggerFactory(LoggerFactoryPtr loggerFactory) { s_loggerFactory = loggerFactory; }

LoggerFactoryPtr LogUtils::getLoggerFactory() {
    if (!s_loggerFactory) {
        s_loggerFactory.reset(new SimpleLoggerFactory());
    }
    return s_loggerFactory;
}

std::string LogUtils::getLoggerName(const std::string& path) {
    // Remove all directories from filename
    int startIdx = path.find_last_of("/");
    int endIdx = path.find_last_of(".");
    return path.substr(startIdx + 1, endIdx - startIdx - 1);
}

}  // namespace pulsar