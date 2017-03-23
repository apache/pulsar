//
// Created by Jai Asher on 3/22/17.
//

#ifndef PULSAR_CPP_HTTPLOOKUPSERVICE_H
#define PULSAR_CPP_HTTPLOOKUPSERVICE_H

#include <lib/LookupService.h>
#include <lib/ClientImpl.h>


namespace pulsar {

    class HTTPLookupService : public LookupService {
    public:
        HTTPLookupService(const std::string& lookupUrl, const ClientConfiguration& clientConfiguration,
                      ExecutorServiceProviderPtr executorProvider, const AuthenticationPtr& authData);

        Future<Result, LookupDataResultPtr> lookupAsync(const std::string& destinationName);

        Future<Result, LookupDataResultPtr> getPartitionMetadataAsync(const DestinationNamePtr& dn);

    private:
        ExecutorServiceProviderPtr executorProvider_;
        std::string lookupUrl_;
        std::string adminUrl_;
        AuthenticationPtr authenticationPtr_;
        TimeDuration lookupTimeout_;
    };

}

#endif //PULSAR_CPP_HTTPLOOKUPSERVICE_H
