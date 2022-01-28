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

#include "Log4CxxLogger.h"
#include <iostream>

#ifdef USE_LOG4CXX

#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/consoleappender.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/patternlayout.h>

using namespace log4cxx;

namespace pulsar {

class Log4CxxLogger : public Logger {
    std::string _fileName;
    LoggerPtr _logger;

   public:
    Log4CxxLogger(const std::string &fileName)
        : _fileName(fileName), _logger(log4cxx::Logger::getLogger(LOG_CATEGORY_NAME + fileName)) {}

    bool isEnabled(Level level) { return _logger->isEnabledFor(getLevel(level)); }

    void log(Level level, int line, const std::string &message) {
        spi::LocationInfo location(_fileName.c_str(), "", line);
        _logger->forcedLogLS(getLevel(level), message, location);
    }

   private:
    static log4cxx::LevelPtr getLevel(Level level) {
        switch (level) {
            case LEVEL_DEBUG:
                return log4cxx::Level::getDebug();
            case LEVEL_INFO:
                return log4cxx::Level::getInfo();
            case LEVEL_WARN:
                return log4cxx::Level::getWarn();
            case LEVEL_ERROR:
                return log4cxx::Level::getError();
        }
    }
};

std::unique_ptr<LoggerFactory> Log4CxxLoggerFactory::create() {
    if (!LogManager::getLoggerRepository()->isConfigured()) {
        LogManager::getLoggerRepository()->setConfigured(true);
        LoggerPtr root = log4cxx::Logger::getRootLogger();
        static const LogString TTCC_CONVERSION_PATTERN(
            LOG4CXX_STR("%d{yyyy-MM-dd HH:mm:ss,SSS Z} [%t] %-5p %l - %m%n"));
        LayoutPtr layout(new PatternLayout(TTCC_CONVERSION_PATTERN));
        AppenderPtr appender(new ConsoleAppender(layout));
        root->setLevel(log4cxx::Level::getInfo());
        root->addAppender(appender);
    }

    return std::unique_ptr<LoggerFactory>(new Log4CxxLoggerFactory());
}

std::unique_ptr<LoggerFactory> Log4CxxLoggerFactory::create(const std::string &log4cxxConfFile) {
    try {
        log4cxx::PropertyConfigurator::configure(log4cxxConfFile);
    } catch (const std::exception &e) {
        std::cerr << "exception caught while configuring log4cpp via '" << log4cxxConfFile
                  << "': " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "unknown exception while configuring log4cpp via '" << log4cxxConfFile << "'."
                  << std::endl;
    }

    return std::unique_ptr<LoggerFactory>(new Log4CxxLoggerFactory());
}

Logger *Log4CxxLoggerFactory::getLogger(const std::string &fileName) { return new Log4CxxLogger(fileName); }
}  // namespace pulsar

#endif  // USE_LOG4CXX
