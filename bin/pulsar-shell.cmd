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

for %%i in ("%~dp0.") do SET "SCRIPT_PATH=%%~fi"
set "PULSAR_HOME_DIR=%SCRIPT_PATH%\..\"
for %%i in ("%PULSAR_HOME_DIR%.") do SET "PULSAR_HOME=%%~fi"
call "%PULSAR_HOME%\bin\pulsar-admin-common.cmd"
if ERRORLEVEL 1 (
  exit /b 1
)

set "OPTS=%OPTS% -Dorg.jline.terminal.jansi=false"
set "OPTS=%OPTS% -Dpulsar.shell.config.default=%cd%"
set "DEFAULT_CONFIG=-Dpulsar.shell.config.default="%PULSAR_CLIENT_CONF%""
cd "%PULSAR_HOME%"
"%JAVACMD%" %OPTS%  %DEFAULT_CONFIG%  org.apache.pulsar.shell.PulsarShell %*