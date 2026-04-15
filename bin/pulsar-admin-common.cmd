@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM   http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off

if not defined JAVA_HOME (
  for %%i in (java.exe) do set "JAVACMD=%%~$PATH:i"
) else (
  set "JAVACMD=%JAVA_HOME%\bin\java.exe"
)

if not exist "%JAVACMD%" (
  echo The JAVA_HOME environment variable is not defined correctly, so Pulsar CLI cannot be started. >&2
  echo JAVA_HOME is set to "%JAVA_HOME%", but "%JAVACMD%" does not exist. >&2
  exit /b 1
)

set JAVA_MAJOR_VERSION=0
REM Requires "setlocal enabledelayedexpansion" to work
for /f tokens^=3 %%g in ('"!JAVACMD!" -version 2^>^&1 ^| findstr /i version') do (
  set JAVA_MAJOR_VERSION=%%g
)
set JAVA_MAJOR_VERSION=%JAVA_MAJOR_VERSION:"=%
for /f "delims=.-_ tokens=1-2" %%v in ("%JAVA_MAJOR_VERSION%") do (
  if /I "%%v" EQU "1" (
    set JAVA_MAJOR_VERSION=%%w
  ) else (
    set JAVA_MAJOR_VERSION=%%v
  )
)

for %%i in ("%~dp0.") do SET "SCRIPT_PATH=%%~fi"
set "PULSAR_HOME_DIR=%SCRIPT_PATH%\..\"
for %%i in ("%PULSAR_HOME_DIR%.") do SET "PULSAR_HOME=%%~fi"
set "PULSAR_CLASSPATH=%PULSAR_CLASSPATH%;%PULSAR_HOME%\lib\*"

if "%PULSAR_CLIENT_CONF%" == "" set "PULSAR_CLIENT_CONF=%PULSAR_HOME%\conf\client.conf"
if "%PULSAR_LOG_CONF%" == "" set "PULSAR_LOG_CONF=%PULSAR_HOME%\conf\log4j2.yaml"

set "PULSAR_LOG_CONF_DIR1=%PULSAR_LOG_CONF%\..\"
for %%i in ("%PULSAR_LOG_CONF_DIR1%.") do SET "PULSAR_LOG_CONF_DIR=%%~fi"
for %%a in ("%PULSAR_LOG_CONF%") do SET "PULSAR_LOG_CONF_BASENAME=%%~nxa"

set "PULSAR_CLASSPATH=%PULSAR_CLASSPATH%;%PULSAR_LOG_CONF_DIR%"

set "OPTS=%OPTS% -Dlog4j.configurationFile="%PULSAR_LOG_CONF_BASENAME%""
set "OPTS=-Djava.net.preferIPv4Stack=true %OPTS%"

REM Allow Netty to use reflection access
set "OPTS=%OPTS% -Dio.netty.tryReflectionSetAccessible=true"
set "OPTS=%OPTS% -Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true"

if %JAVA_MAJOR_VERSION% GTR 23 (
  REM Required to allow sun.misc.Unsafe on JDK 24 without warnings
  REM Also required for enabling unsafe memory access for Netty since 4.1.121.Final
  set "OPTS=--sun-misc-unsafe-memory-access=allow %OPTS%"
)

if %JAVA_MAJOR_VERSION% GTR 8 (
  set "OPTS=%OPTS% --add-opens java.base/sun.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"
  REM Required by Pulsar client optimized checksum calculation on other than Linux x86_64 platforms
  REM reflection access to java.util.zip.CRC32C
  set "OPTS=%OPTS% --add-opens java.base/java.util.zip=ALL-UNNAMED"
)

if %JAVA_MAJOR_VERSION% GEQ 11 (
  REM Required by Netty for optimized direct byte buffer access
  set "OPTS=%OPTS% --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED"
)
REM These two settings work together to ensure the Pulsar process exits immediately and predictably
REM if it runs out of either Java heap memory or its internal off-heap memory,
REM as these are unrecoverable errors that require a process restart to clear the faulty state and restore operation
set "OPTS=-Dpulsar.allocator.exit_on_oom=true %OPTS%"
REM Netty tuning
REM These settings are primarily used to modify the Netty allocator configuration,
REM improving memory utilization and reducing the frequency of requesting off-heap memory from the OS
REM
REM Based on the netty source code, the allocator's default chunk size is calculated as:
REM io.netty.allocator.pageSize (default: 8192) shifted left by
REM io.netty.allocator.maxOrder (default: 9 after Netty 4.1.76.Final version).
REM This equals 8192 * 2^9 = 4 MB：
REM https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java#L105
REM
REM Allocations that are larger than chunk size are considered huge allocations and don't use the pool:
REM https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/PoolArena.java#L141-L142
REM
REM Currently, Pulsar defaults to a maximum single message size of 5 MB.
REM Therefore, when frequently producing messages whose size exceeds the chunk size,
REM Netty cannot utilize resources from the memory pool and must frequently allocate native memory.
REM This can lead to increased physical memory fragmentation and higher reclamation costs.
REM Thus, increasing io.netty.allocator.maxOrder to 10 to ensure that a single message is larger
REM than chunk size (8MB) and can reuse Netty's memory pool.
set "OPTS=-Dio.netty.recycler.maxCapacityPerThread=4096 -Dio.netty.allocator.maxOrder=10 %OPTS%"

set "OPTS=-cp "%PULSAR_CLASSPATH%" %OPTS%"
set "OPTS=%OPTS% %PULSAR_EXTRA_OPTS%"

if "%PULSAR_LOG_DIR%" == "" set "PULSAR_LOG_DIR=%PULSAR_HOME%\logs"
if "%PULSAR_LOG_APPENDER%" == "" set "PULSAR_LOG_APPENDER=RoutingAppender"
REM PULSAR_LOG_CONSOLE_JSON_TEMPLATE is deprecated -- use PULSAR_LOG_JSON_TEMPLATE
if "%PULSAR_LOG_JSON_TEMPLATE%" == "" set "PULSAR_LOG_JSON_TEMPLATE=%PULSAR_LOG_CONSOLE_JSON_TEMPLATE%"
if "%PULSAR_LOG_JSON_TEMPLATE%" == "" set "PULSAR_LOG_JSON_TEMPLATE=classpath:EcsLayout.json"
if "%PULSAR_LOG_FORMAT%" == "" set "PULSAR_LOG_FORMAT=text"
if "%PULSAR_ROUTING_APPENDER_DEFAULT%" == "" set "PULSAR_ROUTING_APPENDER_DEFAULT=Console"
if "%PULSAR_LOG_IMMEDIATE_FLUSH%" == "" set "PULSAR_LOG_IMMEDIATE_FLUSH=false"

set "OPTS=%OPTS% -Dpulsar.log.appender=%PULSAR_LOG_APPENDER%"
set "OPTS=%OPTS% -Dpulsar.log.json.template=%PULSAR_LOG_JSON_TEMPLATE%"
set "OPTS=%OPTS% -Dpulsar.log.format=%PULSAR_LOG_FORMAT%"
set "OPTS=%OPTS% -Dpulsar.log.dir=%PULSAR_LOG_DIR%"
if not "%PULSAR_LOG_LEVEL%" == "" set "OPTS=%OPTS% -Dpulsar.log.level=%PULSAR_LOG_LEVEL%"
if not "%PULSAR_LOG_ROOT_LEVEL%" == "" (
    set "OPTS=%OPTS% -Dpulsar.log.root.level=%PULSAR_LOG_ROOT_LEVEL%"
) else (
    if not "%PULSAR_LOG_LEVEL%" == "" (
        set "OPTS=%OPTS% -Dpulsar.log.root.level=%PULSAR_LOG_LEVEL%"
    )
)
set "OPTS=%OPTS% -Dpulsar.log.immediateFlush=%PULSAR_LOG_IMMEDIATE_FLUSH%"
set "OPTS=%OPTS% -Dpulsar.routing.appender.default=%PULSAR_ROUTING_APPENDER_DEFAULT%"