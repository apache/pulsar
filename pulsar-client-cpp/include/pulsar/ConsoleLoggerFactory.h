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

class ConsoleLoggerFactoryImpl;

/**
 * The default LoggerFactory of Client if `USE_LOG4CXX` macro was not defined during compilation.
 *
 *
 * The log format is "yyyy-MM-dd HH:mm:ss,SSS Z <level> <thread-id> <file>:<line> | <msg>", like
 *
 * ```
 * 2021-03-24 17:35:46,571 +0800 INFO  [0x10a951e00] ConnectionPool:85 | Created connection for ...
 * ```
 *
 * It uses `std::cout` to prints logs to standard output. You can use this factory class to change your log
 * level simply.
 *
 * ```c++
 * #include <pulsar/ConsoleLoggerFactory.h>
 *
 * ClientConfiguration conf;
 * conf.setLogger(new ConsoleLoggerFactory(Logger::LEVEL_DEBUG));
 * Client client("pulsar://localhost:6650", conf);
 * ```
 */
class PULSAR_PUBLIC ConsoleLoggerFactory : public LoggerFactory {
   public:
    explicit ConsoleLoggerFactory(Logger::Level level = Logger::LEVEL_INFO);

    ~ConsoleLoggerFactory();

    Logger* getLogger(const std::string& fileName) override;

   private:
    std::unique_ptr<ConsoleLoggerFactoryImpl> impl_;
};

}  // namespace pulsar
