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

import junit.framework.TestResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.notification.server.NotificationServer;
import org.apache.activemq.broker.BrokerService;



public class MultiVmNotificationsTestSuite extends MultiVmTestSuite
{
    private static Logger logger = LoggerFactory.getLogger(MultiVmNotificationsTestSuite.class.getName());
    private static NotificationServer notificationServer;
    private static BrokerService brokerService;
    private String url = "tcp://localhost:";
    public MultiVmNotificationsTestSuite(Class testClass)
    {
        super(testClass);
    }

    public void run(TestResult result)
    {
        this.startMessagingSystem();
        super.run(result);
        this.stopMessagingSystem();
    }

    public static Logger getLogger()
    {
        return logger;
    }

    private void stopMessagingSystem()
    {
        try
        {
            if (notificationServer != null)
            {
                notificationServer.shutdown();
            }
            if (brokerService != null)
            {
                brokerService.stop();
            }
        }
        catch (Exception e)
        {
            getLogger().warn("Exception while stopping ActiveMq Messaging Container", e);
        }
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
