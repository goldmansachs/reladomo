#!/bin/bash

# Copyright 2016 Goldman Sachs.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if [ -z "$RELADOMO_HOME" ]
then
    export RELADOMO_HOME=`cd $(dirname $0)/.. && pwd`
fi

echo RELADOMO_HOME is $RELADOMO_HOME

# Updated JDK detection logic
if [ -n "$RELADOMO_JDK_HOME" ]; then
  export JDK_HOME=$RELADOMO_JDK_HOME
elif [ -d "/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home" ]; then
  export JDK_HOME="/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home"
else
  # Default to system Java if specific version not found
  export JDK_HOME=$JAVA_HOME
fi

export GENERATE_RELADOMO_CONCRETE_CLASSES=true

export RELADOMO_TEST_DATABASE=H2
