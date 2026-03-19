-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Create keyspace for air quality data
CREATE KEYSPACE IF NOT EXISTS airquality
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Use the airquality keyspace
USE airquality;

-- Create observation table
CREATE TABLE IF NOT EXISTS observation (
    key text PRIMARY KEY,
    observed text
);

-- Create reading table with air quality measurements
CREATE TABLE IF NOT EXISTS reading (
    reporting_area text,
    date_observed text,
    hour_observed int,
    readingid text,
    avg_ozone double,
    min_ozone double,
    max_ozone double,
    avg_pm10 double,
    min_pm10 double,
    max_pm10 double,
    avg_pm25 double,
    min_pm25 double,
    max_pm25 double,
    local_time_zone text,
    state_code text,
    latitude float,
    longitude float,
    PRIMARY KEY ((reporting_area), date_observed, hour_observed)
) WITH CLUSTERING ORDER BY (date_observed DESC, hour_observed DESC);

-- Create index on readingid for queries by reading ID
CREATE INDEX IF NOT EXISTS reading_id_idx ON reading (readingid);

-- Create index on state_code for queries by state
CREATE INDEX IF NOT EXISTS state_code_idx ON reading (state_code);
