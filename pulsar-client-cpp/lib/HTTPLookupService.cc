#include <lib/HTTPLookupService.h>

DECLARE_LOG_OBJECT()

namespace pulsar {
    using boost::posix_time::seconds;

    const static std::string V2_PATH = "/lookup/v2/destination/";
    const static std::string PARTITION_PATH = "/admin/persistent/";
    const static int MAX_HTTP_REDIRECTS = 20;
    const static char SEPARATOR = '/';
    const static std::string PARTITION_METHOD_NAME = "partitions";

    HTTPLookupService::HTTPLookupService(const std::string& lookupUrl,
                                 const ClientConfiguration& clientConfiguration,
                                 ExecutorServiceProviderPtr executorProvider,
                                 const AuthenticationPtr& authData)
            : executorProvider_(executorProvider),
              authenticationPtr_(authData),
              lookupTimeout_(seconds(clientConfiguration.getOperationTimeoutSeconds())) {
        if (lookupUrl[lookupUrl.length() - 1] == SEPARATOR) {
            // Remove trailing '/'
            lookupUrl_ = lookupUrl.substr(0, lookupUrl.length() - 1);
        } else {
            lookupUrl_ = lookupUrl;
        }
        adminUrl_ = lookupUrl_;
        lookupUrl_ += V2_PATH;
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }

    static size_t CurlWrite_CallbackFunc_StdString(void *contents, size_t size, size_t nmemb, std::string *s)
    {
        size_t newLength = size*nmemb;
        size_t oldLength = s->size();
        try
        {
            s->resize(oldLength + newLength);
        }
        catch(std::bad_alloc &e)
        {
            //handle memory problem
            return 0;
        }

        std::copy((char*)contents,(char*)contents+newLength,s->begin()+oldLength);
        return size*nmemb;
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::lookupAsync(const std::string& destinationName) {
        boost::shared_ptr<DestinationName> dn = DestinationName::get(destinationName);

        Promise<Result, LookupDataResultPtr> promise;
        if (!dn) {
            LOG_ERROR("Unable to parse destination - " << destinationName);
            promise.setFailed(ResultInvalidTopicName);
            return promise.getFuture();
        }

        std::string requestUrl = lookupUrl_ + dn->getLookupName();
        LOG_DEBUG("Doing topic lookup on: " << requestUrl);

        // LookupDataPromisePtr promise = boost::make_shared<LookupDataPromise>();
        // cmsAdminGetAsync(requestUrl, promise, PartitionMetadataPromisePtr());
        // return promise->getFuture();

        AuthenticationDataPtr authDataContent;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication method should have AuthenticationData");
            promise.setFailed(authResult);
            return promise.getFuture();
        }

        std::string authorizationData;
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }

        std::string response;

        struct curl_slist *list = NULL;
        list = curl_slist_append(list, "Content-Type: application/json");
        list = curl_slist_append(list, authorizationData.c_str());

        CURL *eh = curl_easy_init();
        CURLcode res;

        if (!eh) {
            LOG_ERROR("curl_easy_init() failed: ");
            promise.setFailed(ResultUnknownError);
            return promise.getFuture();
        }
        curl_easy_setopt(eh, CURLOPT_URL, requestUrl.c_str());
        curl_easy_setopt(eh, CURLOPT_HTTPHEADER, list);
        curl_easy_setopt(eh, CURLOPT_WRITEFUNCTION, CurlWrite_CallbackFunc_StdString);
        curl_easy_setopt(eh, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(eh, CURLOPT_VERBOSE, 1L);
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(eh);
        /* Check for errors */
        if(res != CURLE_OK)
        {
            LOG_ERROR("curl_easy_perform() failed: " << curl_easy_strerror(res));
            promise.setFailed(ResultUnknownError);
            return promise.getFuture();
        }

        curl_slist_free_all(list);
        /* always cleanup */
        curl_easy_cleanup(eh);
        LOG_ERROR("Lookup Response: "<<response);
        return promise.getFuture();
    }

    Future<Result, LookupDataResultPtr> HTTPLookupService::getPartitionMetadataAsync(const DestinationNamePtr& dn) {
        std::stringstream requestUrl;
        requestUrl << adminUrl_ << PARTITION_PATH << dn->getProperty() << SEPARATOR << dn->getCluster()
                   << SEPARATOR << dn->getNamespacePortion() << SEPARATOR << dn->getEncodedLocalName() << SEPARATOR
                   << PARTITION_METHOD_NAME;
        LOG_DEBUG("Getting Partition Metadata from : " << requestUrl.str());

        // PartitionMetadataPromisePtr promise = boost::make_shared<PartitionMetadataPromise>();
        // cmsAdminGetAsync(requestUrl.str(), LookupDataPromisePtr(), promise);
        // return promise->getFuture();

        AuthenticationDataPtr authDataContent;
        Promise<Result, LookupDataResultPtr> promise;
        Result authResult = authenticationPtr_->getAuthData(authDataContent);
        if (authResult != ResultOk) {
            LOG_ERROR("All Authentication method should have AuthenticationData");
            promise.setFailed(authResult);
            return promise.getFuture();
        }

        std::string authorizationData;
        if (authDataContent->hasDataFromCommand()) {
            authorizationData = authDataContent->getCommandData();
        }

        std::string response;

        struct curl_slist *list = NULL;
        list = curl_slist_append(list, "Content-Type: application/json");
        list = curl_slist_append(list, authorizationData.c_str());

        CURL *eh = curl_easy_init();
        CURLcode res;

        if (!eh) {
            promise.setFailed(ResultUnknownError);
            return promise.getFuture();
        }



        curl_easy_setopt(eh, CURLOPT_URL, requestUrl.str().c_str());
        curl_easy_setopt(eh, CURLOPT_HTTPHEADER, list);
        curl_easy_setopt(eh, CURLOPT_WRITEFUNCTION, CurlWrite_CallbackFunc_StdString);
        curl_easy_setopt(eh, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(eh, CURLOPT_VERBOSE, 1L);
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(eh);
        /* Check for errors */
        if(res != CURLE_OK)
        {
            LOG_ERROR("curl_easy_perform() failed: " << curl_easy_strerror(res));
            promise.setFailed(ResultUnknownError);
            return promise.getFuture();
        }

        curl_slist_free_all(list);
        /* always cleanup */
        curl_easy_cleanup(eh);

        LOG_ERROR("Lookup Response: "<<response);
        return promise.getFuture();
    }

////private
//    void HTTPLookupService::cmsAdminGetAsync(const std::string& requestUrl,
//                                         LookupDataPromisePtr lookupDataPromise,
//                                         PartitionMetadataPromisePtr partitionMetdataPromise) {
//        //check if the URL is valid or not if not return invalid address error
//        // asio::error_code err;
//        urdl::url service_url = urdl::url::from_string(requestUrl, err);
//        if (err) {
//            LOG_ERROR("Invalid Url, unable to parse - " << requestUrl << "error - " << err);
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultInvalidUrl);
//            }
//            if (partitionMetdataPromise) {
//                partitionMetdataPromise->setFailed(ResultInvalidUrl);
//            }
//            return;
//        }
//
//        AuthenticationDataPtr authDataContent;
//        Result authResult = authentication->getAuthData(authDataContent);
//        if (authResult != ResultOk) {
//            LOG_ERROR("All Authentication method should have AuthenticationData");
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(authResult);
//            }
//            if (partitionMetdataPromise) {
//                partitionMetdataPromise->setFailed(authResult);
//            }
//            return;
//        }
//
//        std::string authorizationData;
//        if (authDataContent->hasDataFromCommand()) {
//            authorizationData = authDataContent->getCommandData();
//        }
//
//        // Setup timer
//        DeadlineTimerPtr timer = executorProvider_->get()->createDeadlineTimer();
//        timer->expires_from_now(lookupTimeout_);
//        timer->async_wait(boost::bind(&HTTPLookupService::handleTimeout, this, _1, lookupDataPromise, partitionMetdataPromise));
//
//        ReadStreamPtr readStream = executorProvider_->get()->createReadStream();
//        urdl::option_set options;
//        options.set_option(urdl::http::max_redirects(MAX_HTTP_REDIRECTS));
//        options.set_option(urdl::http::yca_header(authorizationData));
//        readStream->set_options(options);
//        readStream->async_open(requestUrl,
//                               boost::bind(&HTTPLookupService::handleOpen,
//                                           this, _1, readStream, requestUrl,
//                                           lookupDataPromise, partitionMetdataPromise, timer));
//
//
//    }
//
///*
// * @param ec asio error code, non-zero means trouble
// * handler for async_open call
// */
//    void HTTPLookupService::handleOpen(const asio::error_code& ec, ReadStreamPtr readStream,
//                                   const std::string& requestUrl,
//                                   LookupDataPromisePtr lookupDataPromise,
//                                   PartitionMetadataPromisePtr partitionMetdataPromise, DeadlineTimerPtr timer) {
//        if (!ec) {
//            // composable call, only returns after reading content_length size data
//            size_t length = readStream->content_length();
//            SharedBuffer buffer = SharedBuffer::allocate(length);
//            asio::async_read(*readStream, buffer.asio_buffer(),
//                             boost::bind(&HTTPLookupService::handleRead,
//                                         this, _1, _2, buffer, lookupDataPromise, partitionMetdataPromise, timer));
//        } else {
//            LOG_ERROR("Unable to open URL - " << requestUrl << " : " << ec << " -- " << ec.message());
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultConnectError);
//            } else {
//                partitionMetdataPromise->setFailed(ResultConnectError);
//            }
//
//            timer->cancel();
//        }
//    }
//
///*
// * @param ec asio error_code, non-zero means trouble
// */
//    void HTTPLookupService::handleRead(const asio::error_code& ec, size_t bytesTransferred,
//                                   SharedBuffer buffer,
//                                   LookupDataPromisePtr lookupDataPromise,
//                                   PartitionMetadataPromisePtr partitionMetdataPromise, DeadlineTimerPtr timer) {
//        buffer.bytesWritten(bytesTransferred);
//
//        if (!ec) {
//            std::string response(buffer.data(), buffer.readableBytes());
//            if (lookupDataPromise) {
//                LOG_DEBUG("Lookup response : " << response);
//                lookupDataPromise->setValue(LookupData::parse(response));
//            } else {
//                LOG_DEBUG("Partition Metadata response : " << response);
//                partitionMetdataPromise->setValue(PartitionMetadata::parse(response));
//            }
//        } else {
//            LOG_ERROR("Error Reading data from the socket:  " << ec);
//            if (lookupDataPromise) {
//                lookupDataPromise->setFailed(ResultReadError);
//            } else {
//                partitionMetdataPromise->setFailed(ResultReadError);
//            }
//        }
//
//        timer->cancel();
//    }
//
//    void HTTPLookupService::handleTimeout(const asio::error_code& ec, LookupDataPromisePtr lookupDataPromise,
//                                      PartitionMetadataPromisePtr partitionMetdataPromise) {
//        if (!ec) {
//            // Timer has reached the given timeout
//            if (lookupDataPromise) {
//                if (lookupDataPromise->setFailed(ResultTimeout)) {
//                    LOG_WARN("Timeout during HTTP lookup operation");
//                }
//            } else {
//                if (partitionMetdataPromise->setFailed(ResultTimeout)) {
//                    LOG_WARN("Timeout while fetching partition metadata");
//                }
//            }
//        }
//    }
}
