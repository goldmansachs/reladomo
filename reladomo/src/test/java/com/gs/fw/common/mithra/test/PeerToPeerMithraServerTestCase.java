
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
import com.gs.fw.common.mithra.test.util.MultiVmTestMithraRemoteServerFactory;

import java.sql.Connection;
import java.util.Set;


public class PeerToPeerMithraServerTestCase extends RemoteMithraNotificationTestCase
{

    public void setUp() throws Exception
    {
        MultiVmTestMithraRemoteServerFactory.setPort(this.getApplicationPort1());
        MithraManagerProvider.getMithraManager().setNotificationEventManager(this.createNotificationEventManager());
        String xmlFile = System.getProperty("mithra.xml.config");

        this.setDefaultServerTimezone();

        MithraTestResource mithraTestResource = new MithraTestResource(xmlFile);
        mithraTestResource.setRestrictedClassList(this.getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setPeerToPeer(true);
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        if (!connectionManager.hasConnectionManagerForSource("A"))
        {
            connectionManager.addConnectionManagerForSource("A");
        }
        if (!connectionManager.hasConnectionManagerForSource("B"))
        {
            connectionManager.addConnectionManagerForSource("B");
        }

        this.setDefaultServerTimezone();

        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        mithraManager.readConfiguration(this.getConfigXml(xmlFile));
    }

    public void workerVmSetUp()
    {
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setPeerToPeer(true);
        super.workerVmSetUp();
    }

    protected void addTestClassesFromOther(RemoteMithraServerTestCase otherTest, Set toAdd)
    {
        Class[] otherList = otherTest.getRestrictedClassList();
        if (otherList != null)
        {
            for (int i = 0; i < otherList.length; i++)
            {
                toAdd.add(otherList[i]);
            }
        }
    }

    public static Connection getServerSideConnection ()
    {
        return ConnectionManagerForTests.getInstance().getConnection();
    }
}
