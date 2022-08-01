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
#include <pulsar/Client.h>
#include <pulsar/ConsoleLoggerFactory.h>
#include <LogUtils.h>
#include <gtest/gtest.h>
#include <atomic>
#include <thread>

using namespace pulsar;

static std::vector<std::string> logLines;

class MyTestLogger : public Logger {
   public:
    MyTestLogger(const std::string &fileName) : fileName_(fileName) {}

    bool isEnabled(Level level) override { return true; }

    void log(Level level, int line, const std::string &message) override {
        std::stringstream ss;
        ss << std::this_thread::get_id() << " " << level << " " << fileName_ << ":" << line << " " << message
           << std::endl;
        logLines.emplace_back(ss.str());
    }

   private:
    const std::string fileName_;
};

class MyTestLoggerFactory : public LoggerFactory {
   public:
    Logger *getLogger(const std::string &fileName) override { return new MyTestLogger(fileName); }
};

TEST(CustomLoggerTest, testCustomLogger) {
    // simulate new client created on a different thread (because logging factory is called once per thread)
    std::atomic_int numLogLines{0};
    auto testThread = std::thread([&numLogLines] {
        ClientConfiguration clientConfig;
        auto customLogFactory = new MyTestLoggerFactory();
        clientConfig.setLogger(customLogFactory);
        // reset to previous log factory
        Client client("pulsar://localhost:6650", clientConfig);
        client.close();
        ASSERT_TRUE(logLines.size() > 0);
        for (auto &&line : logLines) {
            std::cout << line;
            std::cout.flush();
        }
        numLogLines = logLines.size();
        LogUtils::resetLoggerFactory();
    });
    testThread.join();

    ClientConfiguration clientConfig;
    Client client("pulsar://localhost:6650", clientConfig);
    client.close();
    // custom logger didn't get any new lines
    ASSERT_EQ(logLines.size(), numLogLines);
}

TEST(CustomLoggerTest, testConsoleLoggerFactory) {
    std::unique_ptr<ConsoleLoggerFactory> factory(new ConsoleLoggerFactory);
    std::unique_ptr<Logger> logger(factory->getLogger(__FILE__));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_DEBUG));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_INFO));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_WARN));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_ERROR));

    factory.reset(new ConsoleLoggerFactory(Logger::LEVEL_DEBUG));
    logger.reset(factory->getLogger(__FILE__));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_DEBUG));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_INFO));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_WARN));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_ERROR));

    factory.reset(new ConsoleLoggerFactory(Logger::LEVEL_WARN));
    logger.reset(factory->getLogger(__FILE__));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_DEBUG));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_INFO));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_WARN));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_ERROR));

    factory.reset(new ConsoleLoggerFactory(Logger::LEVEL_ERROR));
    logger.reset(factory->getLogger(__FILE__));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_DEBUG));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_INFO));
    ASSERT_FALSE(logger->isEnabled(Logger::LEVEL_WARN));
    ASSERT_TRUE(logger->isEnabled(Logger::LEVEL_ERROR));
}
