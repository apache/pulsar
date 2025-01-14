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
  goto error
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

set "isjava8=false"
FOR /F "tokens=*" %%g IN ('"java -version 2>&1"') do (
  echo %%g|find "version" >nul
  if errorlevel 0 (
    echo %%g|find "1.8" >nul
    if errorlevel 0 (
     set "isjava8=true"
    )
  )
)

if "%isjava8%" == "false" set "OPTS=%OPTS% --add-opens java.base/sun.net=ALL-UNNAMED"

set "OPTS=-cp "%PULSAR_CLASSPATH%" %OPTS%"
set "OPTS=%OPTS% %PULSAR_EXTRA_OPTS%"

if "%PULSAR_LOG_DIR%" == "" set "PULSAR_LOG_DIR=%PULSAR_HOME%\logs"
if "%PULSAR_LOG_APPENDER%" == "" set "PULSAR_LOG_APPENDER=RoutingAppender"
if "%PULSAR_ROUTING_APPENDER_DEFAULT%" == "" set "PULSAR_ROUTING_APPENDER_DEFAULT=Console"
if "%PULSAR_LOG_IMMEDIATE_FLUSH%" == "" set "PULSAR_LOG_IMMEDIATE_FLUSH=false"

set "OPTS=%OPTS% -Dpulsar.log.appender=%PULSAR_LOG_APPENDER%"
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

:error
exit /b 1
