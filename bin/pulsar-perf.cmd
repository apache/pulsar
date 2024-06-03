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

if "%JAVA_HOME%" == "" (
  for %%i in (java.exe) do set "JAVACMD=%%~$PATH:i"
) else (
  set "JAVACMD=%JAVA_HOME%\bin\java.exe"
)

if not exist "%JAVACMD%" (
  echo The JAVA_HOME environment variable is not defined correctly, so Pulsar CLI cannot be started. >&2
  echo JAVA_HOME is set to "%JAVA_HOME%", but "%JAVACMD%" does not exist. >&2
  exit /B 1
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
if not "%PULSAR_EXTRA_CLASSPATH%" == "" set "PULSAR_CLASSPATH=%PULSAR_CLASSPATH%;%PULSAR_EXTRA_CLASSPATH%"


if "%PULSAR_PERFTEST_CONF%" == "" set "PULSAR_PERFTEST_CONF=%PULSAR_CLIENT_CONF%"


set "OPTS=%OPTS% -Dlog4j.configurationFile="%PULSAR_LOG_CONF_BASENAME%""
set "OPTS=%OPTS% -Djava.net.preferIPv4Stack=true"


set "OPTS=-cp "%PULSAR_CLASSPATH%" %OPTS%"
set "OPTS=%OPTS% %PULSAR_EXTRA_OPTS%"

if "%PULSAR_LOG_DIR%" == "" set "PULSAR_LOG_DIR=%PULSAR_HOME%\logs"
if "%PULSAR_LOG_FILE%" == "" set "PULSAR_LOG_FILE=pulsar-perftest.log"
if "%PULSAR_LOG_APPENDER%" == "" set "PULSAR_LOG_APPENDER=Console"
if "%PULSAR_LOG_LEVEL%" == "" set "PULSAR_LOG_LEVEL=info"
if "%PULSAR_LOG_ROOT_LEVEL%" == "" set "PULSAR_LOG_ROOT_LEVEL=%PULSAR_LOG_LEVEL%"
if "%PULSAR_LOG_IMMEDIATE_FLUSH%" == "" set "PULSAR_LOG_IMMEDIATE_FLUSH=false"


set "OPTS=%OPTS% -Dpulsar.log.appender=%PULSAR_LOG_APPENDER%"
set "OPTS=%OPTS% -Dpulsar.log.dir=%PULSAR_LOG_DIR%"
set "OPTS=%OPTS% -Dpulsar.log.level=%PULSAR_LOG_LEVEL%"
set "OPTS=%OPTS% -Dpulsar.log.root.level=%PULSAR_LOG_ROOT_LEVEL%"
set "OPTS=%OPTS% -Dpulsar.log.immediateFlush=%PULSAR_LOG_IMMEDIATE_FLUSH%"

"%JAVACMD%" %OPTS% org.apache.pulsar.testclient.PulsarPerfTestTool "%PULSAR_PERFTEST_CONF%" %*



:error
set ERROR_CODE=1
goto :eof



