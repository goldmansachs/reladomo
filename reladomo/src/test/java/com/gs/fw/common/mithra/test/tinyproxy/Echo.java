/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

package com.gs.fw.common.mithra.test.tinyproxy;

public interface Echo
{
    String echo(String input);

    Object echoObject(Object inputObject);

    String echoAndSleep(String input, long sleepInMillis) throws Exception;

    String echoAndSleepNoSetting(String input, long sleepInMillis) throws Exception;

    void throwExpectedException() throws FakeException;

    void throwUnexpectedException();

    ObjectWithSerializationError echoWithException(String input);

    int testUnserializableObject(Object o);
}
