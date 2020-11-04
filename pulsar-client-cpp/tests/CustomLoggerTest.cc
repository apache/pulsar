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
#include <LogUtils.h>
#include <gtest/gtest.h>
#include <lib/SimpleLoggerImpl.h>
#include <thread>

using namespace pulsar;

static std::vector<std::string> logLines;

class MyTestLogger : public Logger {
   public:
    MyTestLogger() = default;

    bool isEnabled(Level level) override { return true; }

    void log(Level level, int line, const std::string &message) override {
        std::stringstream ss;
        ss << " " << level << ":" << line << " " << message << std::endl;
        logLines.emplace_back(ss.str());
    }
};

class MyTestLoggerFactory : public LoggerFactory {
   public:
    Logger *getLogger(const std::string &fileName) override { return logger; }

   private:
    MyTestLogger *logger = new MyTestLogger;
};

TEST(CustomLoggerTest, testCustomLogger) {
    // simulate new client created on a different thread (because logging factory is called once per thread)
    auto testThread = std::thread([] {
        ClientConfiguration clientConfig;
        auto customLogFactory = new MyTestLoggerFactory();
        clientConfig.setLogger(customLogFactory);
        // reset to previous log factory
        Client client("pulsar://localhost:6650", clientConfig);
        client.close();
        ASSERT_EQ(logLines.size(), 2);
        LogUtils::resetLoggerFactory();
    });
    testThread.join();

    ClientConfiguration clientConfig;
    Client client("pulsar://localhost:6650", clientConfig);
    client.close();
    // custom logger didn't get any new lines
    ASSERT_EQ(logLines.size(), 2);
}
