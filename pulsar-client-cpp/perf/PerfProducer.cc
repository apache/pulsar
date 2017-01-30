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

#include <lib/LogUtils.h>
DECLARE_LOG_OBJECT()

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/scoped_array.hpp>
#include <boost/make_shared.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/p_square_quantile.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include <iostream>
#include <fstream>
#include <vector>
#include <pulsar/Client.h>
#include "RateLimiter.h"
#include <pulsar/MessageBuilder.h>
typedef boost::shared_ptr<pulsar::RateLimiter> RateLimiterPtr;

struct Arguments {
    std::string topic;
    double rate;
    int msgSize;
    int numTopics;
    int numProducers;
    int numOfThreadsPerProducer;
    std::string serviceURL;
    int producerQueueSize;
    int ioThreads;
    int listenerThreads;
    long samplingPeriod;
    long numberOfSamples;
    unsigned int batchingMaxMessages;
    long batchingMaxAllowedSizeInBytes;
    long batchingMaxPublishDelayMs;
};

namespace pulsar {
class PulsarFriend {
 public:
    static Client getClient(const std::string& url, const ClientConfiguration conf,
                            bool poolConnections) {
        return Client(url, conf, poolConnections);
    }
};
}

// Stats
unsigned long messagesProduced;
unsigned long bytesProduced;
using namespace boost::accumulators;

typedef accumulator_set<uint64_t, stats<tag::mean, tag::p_square_quantile> > LatencyAccumulator;
LatencyAccumulator e2eLatencyAccumulator(quantile_probability = 0.99);
std::vector<pulsar::Producer> producerList;
std::vector<boost::thread> threadList;

boost::mutex mutex;
typedef boost::unique_lock<boost::mutex> Lock;

typedef std::chrono::high_resolution_clock Clock;

void sendCallback(pulsar::Result result, const pulsar::Message& msg, Clock::time_point& publishTime) {
    LOG_DEBUG("result = " << result);
    assert(result == pulsar::ResultOk);
    uint64_t latencyUsec = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - publishTime).count();
    Lock lock(mutex);
    ++messagesProduced;
    bytesProduced += msg.getLength();
    e2eLatencyAccumulator(latencyUsec);
}

// Start a pulsar producer on a topic and keep producing messages
void runProducer(const Arguments& args, std::string topicName, int threadIndex,
                 RateLimiterPtr limiter, pulsar::Producer& producer) {
    LOG_INFO("Producing messages for topic = " << topicName << ", threadIndex = " << threadIndex);

    boost::scoped_array<char> payload(new char[args.msgSize]);
    memset(payload.get(), 0, args.msgSize);
    pulsar::MessageBuilder builder;
    try {
        while (true) {
            if (args.rate != -1) {
                limiter->aquire();
            }
            pulsar::Message msg = builder.create().setAllocatedContent(payload.get(), args.msgSize).build();

            producer.sendAsync(msg, boost::bind(sendCallback, _1, _2, Clock::now()));
            boost::this_thread::interruption_point();
        }
    } catch(const boost::thread_interrupted&) {
        // Thread interruption request received, break the loop
        LOG_INFO("Thread interrupted. Exiting thread.");
    }
}

void startPerfProducer(const Arguments& args, pulsar::ProducerConfiguration &producerConf, pulsar::Client &client) {
    RateLimiterPtr limiter;
    if (args.rate != -1) {
        limiter = boost::make_shared<pulsar::RateLimiter>(args.rate);
    }

    producerList.resize(args.numTopics * args.numProducers);
    for (int i = 0; i < args.numTopics; i++) {
        std::string topic =
                (args.numTopics == 1) ?
                        args.topic : args.topic + "-" + boost::lexical_cast<std::string>(i);
        LOG_INFO("Adding " << args.numProducers << " producers on topic " << topic);

        for (int j = 0; j < args.numProducers; j++) {
            pulsar::Result result = client.createProducer(topic, producerConf, producerList[i*args.numProducers + j]);
            if (result != pulsar::ResultOk) {
                LOG_ERROR("Couldn't create producer: " << result);
                exit(-1);
            } else {
                LOG_DEBUG("Created Producer at index " << i*args.numProducers + j);
            }

            for (int k = 0; k < args.numOfThreadsPerProducer; k++) {
                threadList.push_back(boost::thread(boost::bind(runProducer, args, topic, k, limiter, producerList[i*args.numProducers + j])));
            }
        }
    }
}

int main(int argc, char** argv) {
    LogUtils::init("conf/log4cxx.conf");

    std::string defaultServiceUrl;

    // First try to read default values from config file if present
    const std::string confFile = "conf/client.conf";

    if (boost::filesystem::exists(confFile)) {
        po::variables_map vm;
        po::options_description confFileDesc;
        confFileDesc.add_options()  //
        ("serviceURL", po::value<std::string>()->default_value("pulsar://localhost:6650"));

        std::ifstream file(confFile.c_str());
        po::store(po::parse_config_file<char>(file, confFileDesc, true), vm);
        po::notify(vm);

        defaultServiceUrl = vm["serviceURL"].as<std::string>();
    }

    Arguments args;

    // Declare the supported options.
    po::positional_options_description positional;
    positional.add("topic", 1);

    po::options_description desc("Allowed options");
    desc.add_options()  //
    ("help,h", "Print this help message")  //
    ("rate,r", po::value<double>(&args.rate)->default_value(100.0),
     "Publish rate msg/s across topics")  //
    ("size,s", po::value<int>(&args.msgSize)->default_value(1024), "Message size")  //

    ("num-topics,t", po::value<int>(&args.numTopics)->default_value(1), "Number of topics")  //

    ("num-producers,n", po::value<int>(&args.numProducers)->default_value(1),
     "Number of producers (per topic)")  //

    ("num-threads-per-producers", po::value<int>(&args.numOfThreadsPerProducer)->default_value(1),
     "Number of threads (per producer)") //

    ("service-url,u", po::value<std::string>(&args.serviceURL)->default_value(defaultServiceUrl),
     "Pulsar Service URL")  //

    ("producer-queue-size,p", po::value<int>(&args.producerQueueSize)->default_value(1000),
     "Max size of producer pending messages queue")  //

    ("io-threads,i", po::value<int>(&args.ioThreads)->default_value(1),
     "Number of IO threads to use")  //

    ("listener-threads,l", po::value<int>(&args.listenerThreads)->default_value(1),
     "Number of listener threads") //

    ("sampling-period", po::value<long>(&args.samplingPeriod)->default_value(20),
     "Time elapsed in seconds before reading are aggregated. Default: 20 sec") //

    ("num-of-samples", po::value<long>(&args.numberOfSamples)->default_value(0),
     "Number of samples to take. Default: 0 (run forever)") //

    ("batch-size", po::value<unsigned int>(&args.batchingMaxMessages)->default_value(1),
            "If batch size == 1 then batching is disabled. Default batch size == 1") //

    ("max-batch-size-in-bytes", po::value<long>(&args.batchingMaxAllowedSizeInBytes)->default_value(128 * 1024),
            "Use only is batch-size > 1, Default is 128 KB") //

    ("max-batch-publish-delay-in-ms", po::value<long>(&args.batchingMaxPublishDelayMs)->default_value(3000),
            "Use only is batch-size > 1, Default is 3 seconds");

    po::options_description hidden;
    hidden.add_options()("topic", po::value<std::string>(&args.topic), "Topic name");

    po::options_description allOptions;
    allOptions.add(desc).add(hidden);

    po::variables_map map;
    try {
        po::store(
                po::command_line_parser(argc, argv).options(allOptions).positional(positional).run(),
                map);
        po::notify(map);
    } catch (const std::exception& e) {
        std::cerr << "Error parsing parameters -- " << e.what() << std::endl << std::endl;
        std::cerr << desc << std::endl;
        return -1;
    }

    if (map.count("help")) {
        std::cerr << desc << std::endl;
        return -1;
    }

    if (map.count("topic") != 1) {
        std::cerr << "Need to specify a topic name. eg: persistent://prop/cluster/ns/my-topic"
                  << std::endl << std::endl;
        std::cerr << desc << std::endl;
        return -1;
    }

    LOG_INFO("--- Producer configuration ---");
    for (po::variables_map::iterator it = map.begin(); it != map.end(); ++it) {
        if (it->second.value().type() == typeid(std::string)) {
            LOG_INFO(it->first << ": " << it->second.as<std::string>());
        } else if (it->second.value().type() == typeid(int)) {
            LOG_INFO(it->first << ": " << it->second.as<int>());
        } else if (it->second.value().type() == typeid(double)) {
            LOG_INFO(it->first << ": " << it->second.as<double>());
        } else if (it->second.value().type() == typeid(long)) {
            LOG_INFO(it->first << ": " << it->second.as<long>());
        } else if (it->second.value().type() == typeid(unsigned int)) {
            LOG_INFO(it->first << ": " << it->second.as<unsigned int>());
        } else {
            LOG_INFO(it->first << ": " << "new data type used, please create an else condition in the code");
        }
    }

    LOG_INFO("------------------------------");
    pulsar::ProducerConfiguration producerConf;
    producerConf.setMaxPendingMessages(args.producerQueueSize);
    if (args.batchingMaxMessages > 1) {
        producerConf.setBatchingEnabled(true);
        producerConf.setBatchingMaxMessages(args.batchingMaxMessages);
        producerConf.setBatchingMaxAllowedSizeInBytes(args.batchingMaxAllowedSizeInBytes);
        producerConf.setBatchingMaxPublishDelayMs(args.batchingMaxPublishDelayMs);
    }
    pulsar::ClientConfiguration conf;
    conf.setIOThreads(args.ioThreads);
    conf.setMessageListenerThreads(args.listenerThreads);

    pulsar::Client client(pulsar::PulsarFriend::getClient(args.serviceURL, conf, false));
    startPerfProducer(args, producerConf, client);

    Clock::time_point oldTime = Clock::now();
    unsigned long totalMessagesProduced = 0;
    while (args.numberOfSamples--) {
        std::this_thread::sleep_for(std::chrono::seconds(args.samplingPeriod));

        Clock::time_point now = Clock::now();
        double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - oldTime).count() / 1e3;

        Lock lock(mutex);
        double rate = messagesProduced / elapsed;
        double throughput = bytesProduced / elapsed / 1024 / 1024 * 8;
        totalMessagesProduced += messagesProduced;
        messagesProduced = 0;
        bytesProduced = 0;

        double latencyAvgMs = mean(e2eLatencyAccumulator) / 1000.0;
        double latency99pctMs = p_square_quantile(e2eLatencyAccumulator) / 1000.0;
        e2eLatencyAccumulator = LatencyAccumulator(quantile_probability = 0.99);
        lock.unlock();

        LOG_INFO("Throughput produced: " << rate << "  msg/s --- " << throughput << " Mbit/s --- "  //
               << "Lat avg: " << latencyAvgMs << " ms -- Lat 99pct: " << latency99pctMs << " ms");
        oldTime = now;
    }
    LOG_INFO("Total messagesProduced = " << totalMessagesProduced + messagesProduced);
    for (int i = 0; i < threadList.size(); i++) {
        threadList[i].interrupt();
        threadList[i].join();
    }
    // Waiting for the sendCallbacks To Complete
    usleep(2 * 1000 * 1000);
    for (int i = 0; i < producerList.size(); i++) {
        producerList[i].close();
    }
    // Waiting for 2 seconds
    usleep(2 * 1000 * 1000);
}
