@echo off

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

setlocal
@REM  Current location of this script
set ANTBUILD_HOME=%~dp0
call %ANTBUILD_HOME%setenv.bat

set ANT_HOME=%ANTBUILD_HOME$

@REM  Set the classpath
set ANT_CLASSPATH=%JDK_HOME%\jre\lib\rt.jar
set ANT_CLASSPATH=%ANT_CLASSPATH%;%ANTBUILD_HOME%lib\*
set ANT_CLASSPATH=%ANT_CLASSPATH%;%JDK_HOME%\lib\tools.jar

set ANT_ARGS=-Dant.home=%ANT_HOME%
set ANT_ARGS=%ANT_ARGS% -Dlog4j.configuration="file:%ANTBUILD_HOME%\log4j.config"
@REM set ANT_ARGS=%ANT_ARGS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

@@REM GC Options: -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps 
set JVM_ARGS=-ms16m -mx1024m -server -XX:+UseParallelGC -XX:MaxHeapFreeRatio=20 -XX:MinHeapFreeRatio=10 -XX:CompileThreshold=100

set PATH=%ANTBUILD_HOME%\build\bin;%PATH%

echo on
%JDK_HOME%\jre\bin\java %JVM_ARGS% -classpath %ANT_CLASSPATH% %ANT_ARGS% org.apache.tools.ant.launch.Launcher -listener org.apache.tools.ant.listener.Log4jListener -f %1 %2 %3 %4 %5 %6 %7 %8
@echo off

endlocal
