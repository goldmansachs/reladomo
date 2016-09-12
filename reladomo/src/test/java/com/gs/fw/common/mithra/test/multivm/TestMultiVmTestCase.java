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

    private boolean slaveVmOnStartupCalled = false;
    private boolean slaveVmSetUpCalled = false;

    public void slaveVmOnStartup()
    {
        this.slaveVmOnStartupCalled = true;
    }

    public void slaveVmSetUp()
    {
        this.slaveVmSetUpCalled = true;
    }

    public boolean slaveVmAllGood()
    {
        return this.slaveVmOnStartupCalled && this.slaveVmSetUpCalled;
    }

    public void testTrivialSlaveVm()
    {
        assertNotNull(this.getRemoteSlaveVm());
        Object result = this.getRemoteSlaveVm().executeMethod("slaveVmAllGood");
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
