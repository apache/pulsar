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

#include <pulsar/Logger.h>

namespace pulsar {

/**
 * The default LoggerFactory of Client if `USE_LOG4CXX` macro was not defined during compilation.
 *
 *
 * The log format is "yyyy-mm-dd hh:MM:ss.xxx <level> <thread-id> <file>:<line> | <msg>", like
 *
 * ```
 * 2021-03-24 17:35:46.571 INFO  [0x10a951e00] ConnectionPool:85 | Created connection for ...
 * ```
 *
 * It uses `std::cout` to prints logs to standard output. You can use this factory class to change your log
 * level simply.
 *
 * ```c++
 * #include <pulsar/SimpleLoggerFactory.h>
 *
 * ClientConfiguration conf;
 * conf.setLogger(new SimpleLoggerFactory(Logger::LEVEL_DEBUG));
 * Client client("pulsar://localhost:6650", conf);
 * ```
 */
class SimpleLoggerFactory : public LoggerFactory {
   public:
    explicit SimpleLoggerFactory() = default;
    explicit SimpleLoggerFactory(Logger::Level level) : level_(level) {}

    Logger* getLogger(const std::string& fileName) override;

   private:
    Logger::Level level_{Logger::LEVEL_INFO};
};

}  // namespace pulsar
