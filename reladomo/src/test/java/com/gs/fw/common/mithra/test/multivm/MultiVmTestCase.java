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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import junit.framework.TestCase;
import junit.framework.TestResult;



public class MultiVmTestCase extends TestCase implements MultiVmTest
{
    private static  Logger logger = LoggerFactory.getLogger(MultiVmTestCase.class.getName());

    private RemoteSlaveVm remoteSlaveVm;
    private int applicationPort1;
    private int applicationPort2;

    public TestResult run()
    {
        this.logger.info("running test: "+this.getClass().getName()+"."+this.getName());
        return super.run();
    }

    public void run(TestResult result)
    {
        this.logger.info("running test: "+this.getClass().getName()+"."+this.getName());
        super.run(result);
    }

    public RemoteSlaveVm getRemoteSlaveVm()
    {
        return remoteSlaveVm;
    }

    public void setRemoteSlaveVm(RemoteSlaveVm remoteSlaveVm)
    {
        this.remoteSlaveVm = remoteSlaveVm;
    }

    public void slaveVmOnStartup()
    {
        // nothing to do. Subclass should override
    }

    public void slaveVmSetUp()
    {
        // nothing to do. Subclass should override
    }

    public void slaveVmTearDown()
    {
        // nothing to do. Subclass should override
    }

    public void setApplicationPorts(int port1, int port2)
    {
        this.applicationPort1 = port1;
        this.applicationPort2 = port2;
    }

    public int getApplicationPort1()
    {
        return applicationPort1;
    }

    public int getApplicationPort2()
    {
        return applicationPort2;
    }

    public void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while(now < target)
        {
            try
            {
                Thread.sleep(target-now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
    }
}
