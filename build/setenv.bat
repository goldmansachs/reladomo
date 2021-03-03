if "%JDK_HOME%" == "" (
    echo JDK_HOME defaulting to C:\devel\jdk1.8.0_144
    set JDK_HOME=C:\devel\jdk1.8.0_144
) else (
    echo JDK_HOME is %JDK_HOME%
)

@REM no need to modify stuff below:

set CUR_DIR=%~dp0
set RELADOMO_HOME=%CUR_DIR%..

@REM  Copyright 2016 Goldman Sachs.
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing,
@REM  software distributed under the License is distributed on an
@REM  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM  KIND, either express or implied.  See the License for the
@REM  specific language governing permissions and limitations
@REM  under the License.
