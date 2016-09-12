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

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.notification.UninitializedNotificationEventManager;
import com.gs.fw.common.mithra.test.multivm.MultiClientVmTest;
import com.gs.fw.common.mithra.test.multivm.RemoteSlaveVm;
import com.gs.fw.common.mithra.test.util.MultiVmTestMithraRemoteServerFactory;


public class RemoteMithraClientTestCase extends RemoteMithraNotificationTestCase implements MultiClientVmTest
{
    private RemoteSlaveVm remoteClientVm;


    public RemoteSlaveVm getRemoteClientVm()
    {
        return remoteClientVm;
    }

    public void setRemoteClientVm(RemoteSlaveVm remoteClientVm)
    {
        this.remoteClientVm = remoteClientVm;
    }

    public void clientVmOnStartup()
    {

    }

    public void clientVmSetUp() throws Exception
    {
        MultiVmTestMithraRemoteServerFactory.setPort(this.getApplicationPort1());
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        mithraManager.setNotificationEventManager(this.createNotificationEventManager());
        mithraManager.readConfiguration(this.getConfigXml("MithraConfigClientCache.xml"));
    }

    public void clientVmTearDown()
    {
        MithraManagerProvider.getMithraManager().getNotificationEventManager().clearNotificationSubscribers();
        MithraManagerProvider.getMithraManager().getNotificationEventManager().shutdown();
        MithraManagerProvider.getMithraManager().setNotificationEventManager(new UninitializedNotificationEventManager());
    }

    protected void setDefaultServerTimezone()
    {
    }

    public void forceSendMessages()
    {
        MithraManagerProvider.getMithraManager().getNotificationEventManager().forceSendNow();
    }

    protected void waitForMessages(int updateClassCount, MithraObjectPortal portal) throws InterruptedException
    {
        boolean arrived = this.timedWaitForUpdateMessages(updateClassCount, portal);
        if (!arrived)
        {
            this.getRemoteClientVm().executeMethod("forceSendMessages");
        }
        sleep(100);
        MithraManagerProvider.getMithraManager().getNotificationEventManager().waitUntilCurrentNotificationTasksAreDone();
        int newUpdateClassCount = portal.getPerClassUpdateCountHolder().getUpdateCount();
        if (newUpdateClassCount <= updateClassCount)
        {
            getLogger().error("***************MITHRA NOTIFICATION MESSAGE WAS NOT RECEIVED NOR PROCESSED*******************");
        }
    }
}
