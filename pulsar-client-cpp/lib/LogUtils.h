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

#include <string>
#include <sstream>
#include <memory>

#include <pulsar/defines.h>
#include <pulsar/Logger.h>

namespace pulsar {

#ifdef __GNUC__
#define PULSAR_UNLIKELY(expr) __builtin_expect(expr, 0)
#else
#define PULSAR_UNLIKELY(expr) (expr)
#endif

#define DECLARE_LOG_OBJECT()                                                                     \
    static pulsar::Logger* logger() {                                                            \
        static thread_local std::unique_ptr<pulsar::Logger> threadSpecificLogPtr;                \
        pulsar::Logger* ptr = threadSpecificLogPtr.get();                                        \
        if (PULSAR_UNLIKELY(!ptr)) {                                                             \
            std::string logger = pulsar::LogUtils::getLoggerName(__FILE__);                      \
            threadSpecificLogPtr.reset(pulsar::LogUtils::getLoggerFactory()->getLogger(logger)); \
            ptr = threadSpecificLogPtr.get();                                                    \
        }                                                                                        \
        return ptr;                                                                              \
    }

#define LOG_DEBUG(message)                                                       \
    {                                                                            \
        if (PULSAR_UNLIKELY(logger()->isEnabled(pulsar::Logger::LEVEL_DEBUG))) { \
            std::stringstream ss;                                                \
            ss << message;                                                       \
            logger()->log(pulsar::Logger::LEVEL_DEBUG, __LINE__, ss.str());      \
        }                                                                        \
    }

#define LOG_INFO(message)                                                  \
    {                                                                      \
        if (logger()->isEnabled(pulsar::Logger::LEVEL_INFO)) {             \
            std::stringstream ss;                                          \
            ss << message;                                                 \
            logger()->log(pulsar::Logger::LEVEL_INFO, __LINE__, ss.str()); \
        }                                                                  \
    }

#define LOG_WARN(message)                                                  \
    {                                                                      \
        if (logger()->isEnabled(pulsar::Logger::LEVEL_WARN)) {             \
            std::stringstream ss;                                          \
            ss << message;                                                 \
            logger()->log(pulsar::Logger::LEVEL_WARN, __LINE__, ss.str()); \
        }                                                                  \
    }

#define LOG_ERROR(message)                                                  \
    {                                                                       \
        if (logger()->isEnabled(pulsar::Logger::LEVEL_ERROR)) {             \
            std::stringstream ss;                                           \
            ss << message;                                                  \
            logger()->log(pulsar::Logger::LEVEL_ERROR, __LINE__, ss.str()); \
        }                                                                   \
    }

class PULSAR_PUBLIC LogUtils {
   public:
    static void init(const std::string& logConfFilePath);

    static void setLoggerFactory(std::unique_ptr<LoggerFactory> loggerFactory);

    static void resetLoggerFactory();

    static LoggerFactory* getLoggerFactory();

    static std::string getLoggerName(const std::string& path);
};

}  // namespace pulsar
