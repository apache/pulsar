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

set ARTIFACT_ID=%1
set JAR_PATH=%cd%/target/%ARTIFACT_ID%.jar
set FILE_PREFIX=META-INF/native

:: echo %JAR_PATH%
:: echo %FILE_PREFIX%

ECHO.
echo ----- Renaming epoll lib in %JAR_PATH% ------
set TMP_DIR=%temp%\tmp_pulsar

rd %TMP_DIR% /s /q
mkdir %TMP_DIR%

set UNZIP_CMD=unzip -q %JAR_PATH% -d %TMP_DIR%
call %UNZIP_CMD%

:: echo %UNZIP_CMD%
:: echo %TMP_DIR%

cd /d %TMP_DIR%/%FILE_PREFIX%

:: Loop through the number of groups
SET Obj_Length=2
SET Obj[0].FROM=libnetty_transport_native_epoll_x86_64.so
SET Obj[0].TO=liborg_apache_pulsar_shade_netty_transport_native_epoll_x86_64.so
SET Obj[1].FROM=libnetty_tcnative_linux_x86_64.so
SET Obj[1].TO=liborg_apache_pulsar_shade_netty_tcnative_linux_x86_64.so
SET Obj_Index=0

:LoopStart
IF %Obj_Index% EQU %Obj_Length% GOTO END

SET Obj_Current.FROM=0
SET Obj_Current.TO=0

FOR /F "usebackq delims==. tokens=1-3" %%I IN (`SET Obj[%Obj_Index%]`) DO (
  SET Obj_Current.%%J=%%K.so
)

echo "Renaming %Obj_Current.FROM% -> %Obj_Current.TO%"
call ren %Obj_Current.FROM% %Obj_Current.TO%

SET /A Obj_Index=%Obj_Index% + 1

GOTO LoopStart
:: Loop end

:END
cd /d %TMP_DIR%

:: Overwrite the original ZIP archive
rd %JAR_PATH% /s /q
set ZIP_CMD=zip -q -r %JAR_PATH% .
:: echo %ZIP_CMD%
call %ZIP_CMD%
:: echo %TMP_DIR%
rd %TMP_DIR% /s /q

exit /b 0
:: echo.&pause&goto:eof