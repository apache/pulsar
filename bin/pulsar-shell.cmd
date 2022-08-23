@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.


if not "%JAVA_HOME%" == "" goto javaHomeSet
for %%i in (java.exe) do set "JAVACMD=%%~$PATH:i"
goto checkJavaCmd

:javaHomeSet
set JAVACMD=%JAVA_HOME%\bin\java.exe

if not exist "%JAVACMD%" (
  echo The JAVA_HOME environment variable is not defined correctly, so Pulsar Shell cannot be started. >&2
  echo JAVA_HOME is set to "%JAVA_HOME%", but "%JAVACMD%" does not exist. >&2
  goto error
)

:checkJavaCmd
if not exist "%JAVACMD%" (
  echo The java.exe command does not exist in PATH nor is JAVA_HOME set, so Pulsar Shell cannot be started. >&2
  goto error
)


for %%i in ("%~dp0.") do SET "SCRIPT_PATH=%%~fi"
set "PULSAR_HOME_DIR=%SCRIPT_PATH%\..\"
for %%i in ("%PULSAR_HOME_DIR%.") do SET "PULSAR_HOME=%%~fi"
set "PULSAR_CLASSPATH=%PULSAR_CLASSPATH%;%PULSAR_HOME%\lib\*"


if "%PULSAR_CLIENT_CONF%" == "" set "PULSAR_CLIENT_CONF=%PULSAR_HOME%/conf/client.conf"
if "%PULSAR_LOG_CONF%" == "" set "PULSAR_LOG_CONF=%PULSAR_HOME%/conf/log4j2.yaml"

set "PULSAR_LOG_CONF_DIR_TMP=%PULSAR_LOG_CONF%\..\"
for %%i in ("%PULSAR_LOG_CONF_DIR_TMP%.") do SET "PULSAR_LOG_CONF_DIR=%%~fi"
for %%a in ("%PULSAR_LOG_CONF%") do SET "PULSAR_LOG_CONF_BASENAME=%%~nxa"

set "PULSAR_CLASSPATH=%PULSAR_CLASSPATH%;%PULSAR_LOG_CONF_DIR%"

set "OPTS=%OPTS% "-Dlog4j.configurationFile=%PULSAR_LOG_CONF_BASENAME%""
set "OPTS=%OPTS% "-Djava.net.preferIPv4Stack=true""

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

if "%isjava8%" == "false" set "OPTS=%OPTS% "--add-opens java.base/sun.net=ALL-UNNAMED""

set "OPTS=-cp "%PULSAR_CLASSPATH% %OPTS%""
set "OPTS=%OPTS% %PULSAR_EXTRA_OPTS%"

if "%PULSAR_LOG_DIR%" == "" set "PULSAR_LOG_DIR=%PULSAR_HOME%/logs"
if "%PULSAR_LOG_APPENDER%" == "" set "PULSAR_LOG_APPENDER=RoutingAppender"
if "%PULSAR_LOG_LEVEL%" == "" set "PULSAR_LOG_LEVEL=info"
if "%PULSAR_LOG_ROOT_LEVEL%" == "" set "PULSAR_LOG_ROOT_LEVEL=%PULSAR_LOG_LEVEL%"
if "%PULSAR_ROUTING_APPENDER_DEFAULT%" == "" set "PULSAR_ROUTING_APPENDER_DEFAULT=Console"

set "OPTS=%OPTS% "-Dpulsar.log.appender=%PULSAR_LOG_APPENDER%""
set "OPTS=%OPTS% "-Dpulsar.log.dir=%PULSAR_LOG_DIR%""
set "OPTS=%OPTS% "-Dpulsar.log.level=%PULSAR_LOG_LEVEL%""
set "OPTS=%OPTS% "-Dpulsar.log.root.level=%PULSAR_LOG_ROOT_LEVEL%""
set "OPTS=%OPTS% "-Dpulsar.routing.appender.default=%PULSAR_ROUTING_APPENDER_DEFAULT%""

set "OPTS=%OPTS% "-Dorg.jline.terminal.jansi=false""
set "DEFAULT_CONFIG="-Dpulsar.shell.config.default=%PULSAR_CLIENT_CONF%""


"%JAVACMD%" %OPTS%  %DEFAULT_CONFIG%  org.apache.pulsar.shell.PulsarShell %*
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
set ERROR_CODE=%ERROR_CODE%