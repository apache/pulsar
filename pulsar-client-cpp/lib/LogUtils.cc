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

#include "LogUtils.h"

#include <log4cxx/logger.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/consoleappender.h>
#include <log4cxx/propertyconfigurator.h>
#include <log4cxx/patternlayout.h>
#include <iostream>

using namespace log4cxx;

void LogUtils::init(const std::string& logfilePath) {
    try {
        if (logfilePath.empty()) {
            if (!LogManager::getLoggerRepository()->isConfigured()) {
                LogManager::getLoggerRepository()->setConfigured(true);
                LoggerPtr root = Logger::getRootLogger();
                static const LogString TTCC_CONVERSION_PATTERN(
                        LOG4CXX_STR("%d{HH:mm:ss.SSS} [%t] %-5p %l - %m%n"));
                LayoutPtr layout(new PatternLayout(TTCC_CONVERSION_PATTERN));
                AppenderPtr appender(new ConsoleAppender(layout));
                root->setLevel(log4cxx::Level::getInfo());
                root->addAppender(appender);
            }
        } else {
            log4cxx::PropertyConfigurator::configure(logfilePath);
        }
    } catch (const std::exception& e) {
        std::cerr << "exception caught while configuring log4cpp via '" << logfilePath << "': "
                  << e.what() << std::endl;
    } catch (...) {
        std::cerr << "unknown exception while configuring log4cpp via '" << logfilePath << "'."
                  << std::endl;
    }
}
