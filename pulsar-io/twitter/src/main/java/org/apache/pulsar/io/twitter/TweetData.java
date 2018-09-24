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

package org.apache.pulsar.io.twitter;

import java.util.List;
import lombok.Data;

@Data
public class TweetData {
    private String createdAt;
    private Long id;
    private String idStr;
    private String text;
    private String source;
    private Boolean truncated;
    private User user;
    private RetweetedStatus retweetedStatus;
    private Boolean isQuoteStatus;
    private Long quoteCount;
    private Long replyCount;
    private Long retweetCount;
    private Long favoriteCount;
    private Boolean favorited;
    private Boolean retweeted;
    private String filterLevel;
    private String lang;
    private String timestampMs;
    private Delete delete;


    @Data
    public static class User {
        private Long id;
        private String idStr;
        private String name;
        private String screenName;
        private String location;
        private String description;
        private String translatorType;
        private Boolean _protected;
        private Boolean verified;
        private Long followersCount;
        private Long friendsCount;
        private Long listedCount;
        private Long favouritesCount;
        private Long statusesCount;
        private String createdAt;
        private Boolean geoEnabled;
        private String lang;
        private Boolean contributorsEnabled;
        private Boolean isTranslator;
        private String profileBackgroundColor;
        private String profileBackgroundImageUrl;
        private String profileBackgroundImageUrlHttps;
        private Boolean profileBackgroundTile;
        private String profileLinkColor;
        private String profileSidebarBorderColor;
        private String profileSidebarFillColor;
        private String profileTextColor;
        private Boolean profileUseBackgroundImage;
        private String profileImageUrl;
        private String profileImageUrlHttps;
        private String profileBannerUrl;
        private Boolean defaultProfile;
        private Boolean defaultProfileImage;
    }
    @Data
    public static class RetweetedStatus {
        private String createdAt;
        private Long id;
        private String idStr;
        private String text;
        private String source;
        private Boolean truncated;
        private User user;
        private Boolean isQuoteStatus;
        private Long quoteCount;
        private Long replyCount;
        private Long retweetCount;
        private Long favoriteCount;
        private Boolean favorited;
        private Boolean retweeted;
        private String filterLevel;
        private String lang;
    }
    @Data
    public static class Status {
        private Long id;
        private String idStr;
        private Long userId;
        private String userIdStr;
    }
    @Data
    public static class Delete {
        private Status status;
        private String timestampMs;
    }
}
