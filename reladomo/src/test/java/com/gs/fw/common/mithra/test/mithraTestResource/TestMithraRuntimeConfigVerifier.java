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

package com.gs.fw.common.mithra.test.mithraTestResource;


import com.gs.fw.common.mithra.test.MithraRuntimeConfigVerifier;
import junit.framework.TestCase;

public class TestMithraRuntimeConfigVerifier extends TestCase
{

    public void testPartial() throws Exception
    {
        MithraRuntimeConfigVerifier verifier = new MithraRuntimeConfigVerifier("reladomo/src/test/resources/MithraConfigPartialCache.xml");
        verifier.verifyClasses();
    }

    public void testFull() throws Exception
    {
        MithraRuntimeConfigVerifier verifier = new MithraRuntimeConfigVerifier("reladomo/src/test/resources/MithraConfigFullCache.xml");
        verifier.verifyClasses();
    }

    public void testTablePartition() throws Exception
    {
        MithraRuntimeConfigVerifier verifier = new MithraRuntimeConfigVerifier("reladomo/src/test/resources/MithraTestTableManagerConfig.xml");
        verifier.verifyClasses();
    }

    public void testClient() throws Exception
    {
        MithraRuntimeConfigVerifier verifier = new MithraRuntimeConfigVerifier("reladomo/src/test/resources/MithraConfigClientCache.xml");
        verifier.verifyClasses();
    }
}
