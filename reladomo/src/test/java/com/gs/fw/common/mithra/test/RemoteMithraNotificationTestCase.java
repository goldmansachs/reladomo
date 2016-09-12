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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.finder.UpdateCountHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.notification.MithraNotificationEventManagerImpl;
import com.gs.fw.common.mithra.notification.UninitializedNotificationEventManager;
import com.gs.fw.common.mithra.notification.server.TcpMessagingAdapterFactory;
import com.gs.fw.common.mithra.test.notification.ActiveMqMessagingAdapterFactory;



public class RemoteMithraNotificationTestCase extends RemoteMithraServerTestCase
{
    static private Logger logger = LoggerFactory.getLogger(RemoteMithraNotificationTestCase.class.getName());
    public Logger getLogger()
    {
        return logger;
    }

    protected void setUp()
    throws Exception
    {
        MithraManagerProvider.getMithraManager().setNotificationEventManager(createNotificationEventManager());
        super.setUp();
    }

    protected MithraNotificationEventManagerImpl createTcpNotificationEventManager()
    {
        return new MithraNotificationEventManagerImpl(new TcpMessagingAdapterFactory("localhost", this.getApplicationPort2()));
    }

    protected MithraNotificationEventManagerImpl createNotificationEventManager()
    {
        return new MithraNotificationEventManagerImpl(new ActiveMqMessagingAdapterFactory(this.getApplicationPort2()));
    }

    public void slaveVmSetUp()
    {
        MithraManagerProvider.getMithraManager().setNotificationEventManager(createNotificationEventManager());
        super.slaveVmSetUp();
    }

    public void tearDown()
    throws Exception
    {
        MithraManagerProvider.getMithraManager().getNotificationEventManager().clearNotificationSubscribers();
        MithraManagerProvider.getMithraManager().getNotificationEventManager().shutdown();
        MithraManagerProvider.getMithraManager().setNotificationEventManager(new UninitializedNotificationEventManager());
        super.tearDown();
    }

    public void slaveVmTearDown()
    {
        MithraManagerProvider.getMithraManager().getNotificationEventManager().clearNotificationSubscribers();
        MithraManagerProvider.getMithraManager().getNotificationEventManager().shutdown();
        MithraManagerProvider.getMithraManager().setNotificationEventManager(new UninitializedNotificationEventManager());
        super.slaveVmTearDown();
    }


    protected void waitForRegistrationToComplete()
            throws InterruptedException
    {
        MithraManagerProvider.getMithraManager().getNotificationEventManager().waitUntilCurrentNotificationTasksAreDone();
    }

    protected void waitForMessages(int updateClassCount, MithraObjectPortal portal)
        throws InterruptedException
    {
        waitForMessages(updateClassCount, portal.getPerClassUpdateCountHolder());
    }

    protected void waitForMessages(int updateClassCount, UpdateCountHolder holder)
        throws InterruptedException
    {
        boolean messageProcessed = timedWaitForUpdateMessages(updateClassCount, holder);

        if(!messageProcessed)
        {
            if(!MithraManagerProvider.getMithraManager().getNotificationEventManager().isQueuedExecutorChannelEmpty())
            {
                getLogger().error("**********************************************************************");
                getLogger().error("***************QUEUED EXECUTOR CHANNEL IS NOT EMPTY*******************");
                getLogger().error("**********************************************************************");

            }
            getLogger().error("***************MITHRA NOTIFICATION MESSAGE WAS NOT RECEIVED NOR PROCESSED*******************");
        }
    }

    protected boolean timedWaitForUpdateMessages(int updateClassCount, MithraObjectPortal portal)
    {
        return timedWaitForUpdateMessages(updateClassCount, portal.getPerClassUpdateCountHolder());
    }

    protected boolean timedWaitForUpdateMessages(int updateClassCount, UpdateCountHolder holder)
    {
        boolean messageProcessed = false;
        for(int j = 0; j < 100; j++)
        {
            this.sleep(100);
            int newUpdateClassCount = holder.getUpdateCount();
            if(newUpdateClassCount > updateClassCount)
            {
                messageProcessed = true;
                break;
            }
        }
        MithraManagerProvider.getMithraManager().getNotificationEventManager().waitUntilCurrentNotificationTasksAreDone();
        return messageProcessed;
    }
}
