#!/bin/sh

#  Copyright 2016 Goldman Sachs.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#
#  Lookup our location. Note the complicated code is to ensure that we get the
#  absolute path from $0 as it may be a symlink.
#
PRG="$0"
while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

#  Get standard environment variables
ANTBUILD_HOME=`dirname "$PRG"`
ANT_HOME=$ANTBUILD_HOME

ANT_CLASSPATH=$JDK_HOME/lib/rt.jar
ANT_CLASSPATH=$ANT_CLASSPATH:$ANTBUILD_HOME/lib/*
ANT_CLASSPATH=$ANT_CLASSPATH:$JDK_HOME/lib/tools.jar
export ANT_CLASSPATH
echo $ANT_CLASSPATH

ANT_ARGS="-Dant.home=$ANT_HOME"
ANT_ARGS="$ANT_ARGS -Dlog4j.configuration=log4j.config"
export ANT_ARGS

JVM_ARGS="-ms16m -mx2000m -server -XX:MaxPermSize=128m -XX:+UseParallelGC -XX:MaxHeapFreeRatio=20 -XX:MinHeapFreeRatio=10 -XX:CompileThreshold=100 -XX:+UseSpinning"
export JVM_ARGS

PATH=$ANTBUILD_HOME/build/bin:$PATH
export PATH

$JDK_HOME/bin/java $JVM_ARGS -classpath $ANT_CLASSPATH $ANT_ARGS org.apache.tools.ant.launch.Launcher -listener org.apache.tools.ant.listener.Log4jListener -f $*

