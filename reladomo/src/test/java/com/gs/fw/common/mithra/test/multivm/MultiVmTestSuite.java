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
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;



public class MultiVmTestSuite extends TestSuite
{
    private static  Logger logger = LoggerFactory.getLogger(MultiVmTestSuite.class.getName());

    protected Class testClass;
    protected WorkerVm workerVm;
    private int applicationPort1;
    private int applicationPort2;

    public static Logger getLogger()
    {
        return logger;
    }

    public MultiVmTestSuite(Class testClass)
    {
        super(testClass);
        this.applicationPort1 = (int)(Math.random()*20000+10000);
        this.applicationPort2 = (int)(Math.random()*20000+10000);
        this.testClass = testClass;
        if (!MultiVmTest.class.isAssignableFrom(testClass))
        {
            throw new UnsupportedOperationException("multi vm test suite must be constructed with a MultiVmTest class");
        }
    }

    public void addTest(Test test)
    {
        if (testClass != null)
            throw new UnsupportedOperationException("multi vm test suite must be constructed with a single test class");
        super.addTest(test);
    }

    public void addTestSuite(Class testClass)
    {
        throw new UnsupportedOperationException("multi vm test suite must be constructed with a single test class");
    }

    public void run(TestResult result)
    {
        try
        {
            createWorkerVm();
            workerVm.startWorkerVm(testClass);
            super.run(result);
        }
        catch(RuntimeException e)
        {
            // we log this here, because junit 3.8 hides this exception
            this.logger.error("had a problem running the test", e);
            throw e;
        }
        finally
        {
            workerVm.shutdownWorkerVm();
        }
    }

    protected void createWorkerVm()
    {
        workerVm = new WorkerVm();
        workerVm.setApplicationPort1(this.getApplicationPort1());
        workerVm.setApplicationPort2(this.getApplicationPort2());
    }

    public int getApplicationPort1()
    {
        return applicationPort1;
    }

    public int getApplicationPort2()
    {
        return applicationPort2;
    }

    public void runTest(Test test, TestResult result)
    {
        MultiVmTest multiVmTest = (MultiVmTest) test;
        multiVmTest.setApplicationPorts(this.applicationPort1, this.applicationPort2);
        RemoteWorkerVm remoteWorkerVm = workerVm.getRemoteWorkerVm();
        multiVmTest.setRemoteWorkerVm(remoteWorkerVm);
        remoteWorkerVm.executeMethod("workerVmSetUp");
        super.runTest(test, result);
        remoteWorkerVm.executeMethod("workerVmTearDown");
    }
}
