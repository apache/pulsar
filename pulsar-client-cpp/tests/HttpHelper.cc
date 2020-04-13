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
#include "HttpHelper.h"

#include <curl/curl.h>

static int makeRequest(const std::string& method, const std::string& url, const std::string& body) {
    CURL* curl = curl_easy_init();

    struct curl_slist* list = NULL;

    list = curl_slist_append(list, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    if (!body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    }
    int res = curl_easy_perform(curl);
    curl_slist_free_all(list); /* free the list again */

    if (res != CURLE_OK) {
        return -1;
    }

    long httpResult = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpResult);
    curl_easy_cleanup(curl);
    return (int)httpResult;
}

int makePutRequest(const std::string& url, const std::string& body) { return makeRequest("PUT", url, body); }

int makePostRequest(const std::string& url, const std::string& body) {
    return makeRequest("POST", url, body);
}

int makeDeleteRequest(const std::string& url) { return makeRequest("DELETE", url, ""); }
