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

#include <fstream>
#include <ios>
#include <string>
#include <pulsar/Logger.h>

namespace pulsar {

/**
 * A logger factory that is appending logs to a single file.
 *
 * The log format is "yyyy-mm-dd hh:MM:ss.xxx <level> <thread-id> <file>:<line> | <msg>", like
 *
 * ```
 * 2021-03-24 17:35:46.571 INFO  [0x10a951e00] ConnectionPool:85 | Created connection for ...
 * ```
 *
 * Example:
 *
 * ```c++
 * #include <pulsar/SingleFileLoggerFactory.h>
 *
 * ClientConfiguration conf;
 * conf.setLogger(new SingleFileLoggerFactory(Logger::LEVEL_DEBUG, "pulsar-client-cpp.log"));
 * Client client("pulsar://localhost:6650", conf);
 * ```
 */
class SingleFileLoggerFactory : public pulsar::LoggerFactory {
   public:
    /**
     * Create a SingleFileLoggerFactory instance.
     *
     * @param level the log level
     * @param logFilePath the log file's path
     */
    SingleFileLoggerFactory(Logger::Level level, const std::string& logFilePath)
        : level_(level), os_(logFilePath, std::ios_base::out | std::ios_base::app) {}

    ~SingleFileLoggerFactory() { os_.close(); }

    pulsar::Logger* getLogger(const std::string& filename) override;

   private:
    const pulsar::Logger::Level level_;
    std::ofstream os_;
};

}  // namespace pulsar
