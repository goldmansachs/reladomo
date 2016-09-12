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
import com.gs.fw.common.mithra.notification.server.NotificationServer;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.apache.activemq.broker.BrokerService;


public class MultiClientVmTestSuite extends TestSuite
{
    private static  Logger logger = LoggerFactory.getLogger(MultiVmTestSuite.class.getName());

    private Class testClass;
    private ClientVm clientVm;
    private SlaveVm slaveVm;
    private static BrokerService brokerService;
    private static NotificationServer notificationServer;
    private String url = "tcp://localhost:";
    private int applicationPort1;
    private int applicationPort2;


    public static Logger getLogger()
    {
        return logger;
    }

    public MultiClientVmTestSuite(Class testClass)
    {
        super(testClass);
        this.testClass = testClass;
        this.applicationPort1 = (int)(Math.random()*20000+10000);
        this.applicationPort2 = (int)(Math.random()*20000+10000);
        if (!MultiClientVmTest.class.isAssignableFrom(testClass))
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
            this.startMessagingSystem();
            this.createSlaveVm();
            this.createClientVm();
            slaveVm.startSlaveVm(testClass);
            clientVm.startSlaveVm(testClass);
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
            clientVm.shutdownSlaveVm();
            slaveVm.shutdownSlaveVm();
            try
            {
                if (brokerService != null)
                {
                    brokerService.stop();
                }
                if (notificationServer != null)
                {
                    notificationServer.shutdown();
                }
            }
            catch(Exception e)
            {
               getLogger().warn("Exception while stopping ActiveMq Messaging Container", e);
            }
        }
    }

    protected void createClientVm()
    {
        clientVm = new ClientVm();
        clientVm.setApplicationPort1(this.applicationPort1);
        clientVm.setApplicationPort2(this.applicationPort2);
    }

    protected void createSlaveVm()
    {
        slaveVm = new SlaveVm();
        slaveVm.setApplicationPort1(this.applicationPort1);
        slaveVm.setApplicationPort2(this.applicationPort2);
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
        MultiClientVmTest multiVmTest = (MultiClientVmTest) test;
        multiVmTest.setApplicationPorts(this.applicationPort1, this.applicationPort2);
        RemoteSlaveVm remoteSlaveVm = slaveVm.getRemoteSlaveVm();
        multiVmTest.setRemoteSlaveVm(remoteSlaveVm);
        RemoteSlaveVm remoteClientVm = clientVm.getRemoteSlaveVm();
        multiVmTest.setRemoteClientVm(remoteClientVm);
        remoteSlaveVm.executeMethod("slaveVmSetUp");
        remoteClientVm.executeMethod("clientVmSetUp");
        super.runTest(test, result);
        remoteClientVm.executeMethod("clientVmTearDown");
        remoteSlaveVm.executeMethod("slaveVmTearDown");
    }

    private void startTcpMessagingSystem()
    {
        notificationServer = new NotificationServer(this.getApplicationPort2());
        notificationServer.start();
        notificationServer.waitForStartup();
    }

    private void startMessagingSystem()
    {
        try
        {
            brokerService = new BrokerService();
            brokerService.addConnector(url+this.getApplicationPort2());
            brokerService.setPersistent(false);
            brokerService.setUseJmx(false);
            brokerService.setBrokerName("MithraTest"+this.getApplicationPort2());
            brokerService.setShutdownOnMasterFailure(false);
            brokerService.start();
        }
        catch(Exception e)
        {
           getLogger().error("Unable to start messaging broker");
           throw new RuntimeException("Exception during messaging broker startup", e);
        }
    }


}
