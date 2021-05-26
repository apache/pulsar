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
#include <string.h>

#include <chrono>
#include <ctime>
#include <fstream>
#include <sstream>
#include <thread>

#include <pulsar/Client.h>

class FileLogger : public pulsar::Logger {
   public:
    FileLogger(std::ofstream& os, Level level, const std::string& filename)
        : os_(os), level_(level), filename_(filename) {}

    bool isEnabled(Level level) override { return level >= level_; }

    void log(Level level, int line, const std::string& message) override {
        std::ostringstream oss;
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        oss << currentTime() << " " << convertLevelToString(level) << " [" << std::this_thread::get_id()
            << "] " << filename_ << ":" << line << " " << message << "\n";
        os_ << oss.str();
        os_.flush();
    }

   private:
    std::ostream& os_;
    const Level level_;
    const std::string filename_;

    static const char* currentTime() {
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        char* s = ctime(&now);  // ctime() returns a string with a newline at the end
        auto newLinePos = strlen(s) - 1;
        s[newLinePos] = '\0';
        return s;
    }

    static const char* convertLevelToString(Level level) {
        switch (level) {
            case Level::LEVEL_DEBUG:
                return "DEBUG";
            case Level::LEVEL_INFO:
                return "INFO";
            case Level::LEVEL_WARN:
                return "WARN";
            case Level::LEVEL_ERROR:
                return "ERROR";
            default:
                return "???";
        }
    }
};

/**
 * A logger factory that is appending logs to a single file.
 */
class SingleFileLoggerFactory : public pulsar::LoggerFactory {
   public:
    /**
     * Create a SingleFileLoggerFactory instance.
     *
     * @param level the log level
     * @param logFilePath the log file's path
     */
    SingleFileLoggerFactory(pulsar::Logger::Level level, const std::string& logFilePath)
        : level_(level), os_(logFilePath, std::ios_base::out | std::ios_base::app) {}

    ~SingleFileLoggerFactory() { os_.close(); }

    virtual pulsar::Logger* getLogger(const std::string& filename) override {
        return new FileLogger(os_, level_, filename);
    }

   private:
    const pulsar::Logger::Level level_;
    std::ofstream os_;
};

using namespace pulsar;

int main(int argc, char* argv[]) {
    ClientConfiguration clientConf;
    // The logs whose level is >= INFO will be written to pulsar-cpp-client.log
    clientConf.setLogger(new SingleFileLoggerFactory(Logger::Level::LEVEL_INFO, "pulsar-cpp-client.log"));

    Client client("pulsar://localhost:6650", clientConf);
    Producer producer;
    client.createProducer("my-topic", producer);  // just to create some logs
    client.close();
    return 0;
}
