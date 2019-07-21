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
#ifndef PULSAR_CLIENTCONFIGURATION_H_
#define PULSAR_CLIENTCONFIGURATION_H_

#include <pulsar/defines.h>
#include <pulsar/Authentication.h>
#include <pulsar/Logger.h>

namespace pulsar {
class PulsarWrapper;
struct ClientConfigurationImpl;
class PULSAR_PUBLIC ClientConfiguration {
   public:
    ClientConfiguration();
    ~ClientConfiguration();
    ClientConfiguration(const ClientConfiguration&);
    ClientConfiguration& operator=(const ClientConfiguration&);

    /**
     * Set the authentication method to be used with the broker
     *
     * @param authentication the authentication data to use
     */
    ClientConfiguration& setAuth(const AuthenticationPtr& authentication);

    /**
     * @return the authentication data
     */
    const Authentication& getAuth() const;

    /**
     * Set timeout on client operations (subscribe, create producer, close, unsubscribe)
     * Default is 30 seconds.
     *
     * @param timeout the timeout after which the operation will be considered as failed
     */
    ClientConfiguration& setOperationTimeoutSeconds(int timeout);

    /**
     * @return the client operations timeout in seconds
     */
    int getOperationTimeoutSeconds() const;

    /**
     * Set the number of IO threads to be used by the Pulsar client. Default is 1
     * thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setIOThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getIOThreads() const;

    /**
     * Set the number of threads to be used by the Pulsar client when delivering messages
     * through message listener. Default is 1 thread per Pulsar client.
     *
     * If using more than 1 thread, messages for distinct MessageListener will be
     * delivered in different threads, however a single MessageListener will always
     * be assigned to the same thread.
     *
     * @param threads number of threads
     */
    ClientConfiguration& setMessageListenerThreads(int threads);

    /**
     * @return the number of IO threads to use
     */
    int getMessageListenerThreads() const;

    /**
     * Number of concurrent lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 50000)</i> It should be configured with higher value only in case of it requires to
     * produce/subscribe on
     * thousands of topic using created {@link PulsarClient}
     *
     * @param concurrentLookupRequest
     */
    ClientConfiguration& setConcurrentLookupRequest(int concurrentLookupRequest);

    /**
     * @return Get configured total allowed concurrent lookup-request.
     */
    int getConcurrentLookupRequest() const;

    /**
     * Initialize the log configuration
     *
     * @param logConfFilePath  path of the configuration file
     * @deprecated
     */
    ClientConfiguration& setLogConfFilePath(const std::string& logConfFilePath);

    /**
     * Get the path of log configuration file (log4cpp)
     */
    const std::string& getLogConfFilePath() const;

    /**
     * Configure a custom logger backend to route of Pulsar client library
     * to a different logger implementation.
     *
     * By default, log messages are printed on standard output.
     */
    ClientConfiguration& setLogger(LoggerFactoryPtr loggerFactory);

    LoggerFactoryPtr getLogger() const;

    ClientConfiguration& setUseTls(bool useTls);
    bool isUseTls() const;

    ClientConfiguration& setTlsTrustCertsFilePath(const std::string& tlsTrustCertsFilePath);
    std::string getTlsTrustCertsFilePath() const;

    ClientConfiguration& setTlsAllowInsecureConnection(bool allowInsecure);
    bool isTlsAllowInsecureConnection() const;

    ClientConfiguration& setValidateHostName(bool validateHostName);
    bool isValidateHostName() const;

    /*
     * Initialize stats interval in seconds. Stats are printed and reset after every 'statsIntervalInSeconds'.
     * Set to 0 in order to disable stats collection.
     */
    ClientConfiguration& setStatsIntervalInSeconds(const unsigned int&);

    /*
     * Get the stats interval set in the client.
     */
    const unsigned int& getStatsIntervalInSeconds() const;

    friend class ClientImpl;
    friend class PulsarWrapper;

   private:
    const AuthenticationPtr& getAuthPtr() const;
    std::shared_ptr<ClientConfigurationImpl> impl_;
};
}  // namespace pulsar

#endif /* PULSAR_CLIENTCONFIGURATION_H_ */
