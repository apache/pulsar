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

#include <iostream>
#include <sstream>
#include <thread>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/format.hpp>

namespace pulsar {

inline std::ostream &operator<<(std::ostream &s, Logger::Level level) {
    switch (level) {
        case Logger::LEVEL_DEBUG:
            s << "DEBUG";
            break;
        case Logger::LEVEL_INFO:
            s << "INFO ";
            break;
        case Logger::LEVEL_WARN:
            s << "WARN ";
            break;
        case Logger::LEVEL_ERROR:
            s << "ERROR";
            break;
    }

    return s;
}

class SimpleLogger : public Logger {
   public:
    SimpleLogger(std::ostream &os, const std::string &filename, Level level)
        : os_(os), filename_(filename), level_(level) {}

    bool isEnabled(Level level) { return level >= level_; }

    void log(Level level, int line, const std::string &message) {
        std::stringstream ss;

        printTimestamp(ss);
        ss << " " << level << " [" << std::this_thread::get_id() << "] " << filename_ << ":" << line << " | "
           << message << "\n";

        os_ << ss.str();
        os_.flush();
    }

   private:
    std::ostream &os_;
    const std::string filename_;
    const Level level_;

    static std::ostream &printTimestamp(std::ostream &s) {
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();

        const boost::format f =
            boost::format("%04d-%02d-%02d %02d:%02d:%02d.%03d") % now.date().year_month_day().year %
            now.date().year_month_day().month.as_number() % now.date().year_month_day().day.as_number() %
            now.time_of_day().hours() % now.time_of_day().minutes() % now.time_of_day().seconds() %
            (now.time_of_day().fractional_seconds() / 1000);

        s << f.str();
        return s;
    }
};

}  // namespace pulsar
