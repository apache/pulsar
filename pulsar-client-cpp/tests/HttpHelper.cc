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

 #include "HttpHelper.h"

 #include <curl/curl.h>


 int makePutRequest(const std::string& url, const std::string& body) {
     CURL* curl = curl_easy_init();
     curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
     curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
     curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());

     int res = curl_easy_perform(curl);
     if (res != CURLE_OK) {
         return -1;
     }

     int httpResult = 0;
     curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpResult);

     curl_easy_cleanup(curl);
     return httpResult;
 }
