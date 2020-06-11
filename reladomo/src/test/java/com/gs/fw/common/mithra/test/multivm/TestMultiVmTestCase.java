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

package com.gs.fw.common.mithra.test.multivm;



public class TestMultiVmTestCase extends MultiVmTestCase
{

    private boolean workerVmOnStartupCalled = false;
    private boolean workerVmSetUpCalled = false;

    public void workerVmOnStartup()
    {
        this.workerVmOnStartupCalled = true;
    }

    public void workerVmSetUp()
    {
        this.workerVmSetUpCalled = true;
    }

    public boolean workerVmAllGood()
    {
        return this.workerVmOnStartupCalled && this.workerVmSetUpCalled;
    }

    public void testTrivialWorkerVm()
    {
        assertNotNull(this.getRemoteWorkerVm());
        Object result = this.getRemoteWorkerVm().executeMethod("workerVmAllGood");
        if (result instanceof Boolean)
        {
            assertTrue(((Boolean)result).booleanValue());
        }
        else
        {
            fail("result class was not a Boolean, but a "+result.getClass().getName());
        }
    }
}
