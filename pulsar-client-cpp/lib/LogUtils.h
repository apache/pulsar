/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LOG_UTIL_H
#define LOG_UTIL_H

#include <boost/thread/tss.hpp>
#include <log4cxx/logger.h>
#include <string>

#define DECLARE_LOG_OBJECT()                                \
    static log4cxx::LoggerPtr& logger()                     \
    {                                                       \
        static boost::thread_specific_ptr<log4cxx::LoggerPtr> threadSpecificLogPtr; \
        log4cxx::LoggerPtr* ptr = threadSpecificLogPtr.get(); \
        if (!ptr) { \
            threadSpecificLogPtr.reset(new log4cxx::LoggerPtr(log4cxx::Logger::getLogger("pulsar." __FILE__)));\
            ptr = threadSpecificLogPtr.get(); \
        } \
        return *ptr;                                      \
    }

#define LOG_DEBUG(message) { \
        if (LOG4CXX_UNLIKELY(logger()->isDebugEnabled())) {\
           ::log4cxx::helpers::MessageBuffer oss_; \
           logger()->forcedLog(::log4cxx::Level::getDebug(), oss_.str(((std::ostream&)oss_) << message), LOG4CXX_LOCATION); }}

#define LOG_INFO(message) { \
        if (logger()->isInfoEnabled()) {\
           ::log4cxx::helpers::MessageBuffer oss_; \
           logger()->forcedLog(::log4cxx::Level::getInfo(), oss_.str(((std::ostream&)oss_) << message), LOG4CXX_LOCATION); }}

#define LOG_WARN(message) { \
        if (logger()->isWarnEnabled()) {\
           ::log4cxx::helpers::MessageBuffer oss_; \
           logger()->forcedLog(::log4cxx::Level::getWarn(), oss_.str(((std::ostream&)oss_) << message), LOG4CXX_LOCATION); }}

#define LOG_ERROR(message) { \
        if (logger()->isErrorEnabled()) {\
           ::log4cxx::helpers::MessageBuffer oss_; \
           logger()->forcedLog(::log4cxx::Level::getError(), oss_.str(((std::ostream&)oss_) << message), LOG4CXX_LOCATION); }}

#define LOG_FATAL(message) { \
        if (logger()->isFatalEnabled()) {\
           ::log4cxx::helpers::MessageBuffer oss_; \
           logger()->forcedLog(::log4cxx::Level::getFatal(), oss_.str(((std::ostream&)oss_) << message), LOG4CXX_LOCATION); }}

#pragma GCC visibility push(default)

class LogUtils {
 public:
    static void init(const std::string& logConfFilePath);
};

#pragma GCC visibility pop

#endif
