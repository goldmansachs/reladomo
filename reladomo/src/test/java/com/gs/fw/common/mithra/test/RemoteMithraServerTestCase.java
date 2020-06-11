
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
import com.gs.fw.common.mithra.test.multivm.MultiVmTestCase;
import com.gs.fw.common.mithra.test.util.MultiVmTestMithraRemoteServerFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspServlet;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.*;

public class RemoteMithraServerTestCase extends MultiVmTestCase
{

    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";
    private Server server;

    private MithraTestResource mithraTestResource;

    public MithraTestResource getMithraTestResource()
    {
        return mithraTestResource;
    }

    public void setMithraTestResource(MithraTestResource mithraTestResource)
    {
        this.mithraTestResource = mithraTestResource;
    }

    protected InputStream getConfigXml(String fileName) throws FileNotFoundException
    {
        String xmlRoot = System.getProperty(MithraTestResource.ROOT_KEY);
        if(xmlRoot == null)
        {
            InputStream result = this.getClass().getClassLoader().getResourceAsStream(fileName);
            if (result == null)
            {
                throw new RuntimeException("could not find "+fileName+" in classpath. Additionally, "+MithraTestResource.ROOT_KEY+" was not specified");
            }
            return result;
        }
        else
        {
            String fullPath = xmlRoot;

            if (!xmlRoot.endsWith(File.separator))
            {
                fullPath += File.separator;
            }
            return new FileInputStream(fullPath + fileName);
        }
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        MultiVmTestMithraRemoteServerFactory.setPort(this.getApplicationPort1());
        MithraManager mithraManager = MithraManagerProvider.getMithraManager();
        mithraManager.readConfiguration(this.getConfigXml("MithraConfigClientCache.xml"));
    }

    protected void tearDown() throws Exception
    {
        super.tearDown();
        MithraManagerProvider.getMithraManager().cleanUpRuntimeCacheControllers();
    }

    public void workerVmOnStartup()
    {
        setupPspMithraService();
    }

    protected void setupPspMithraService()
    {
        server = new Server(this.getApplicationPort1());
        Context context = new Context (server,"/",Context.SESSIONS);
        ServletHolder holder = context.addServlet(PspServlet.class, "/PspServlet");
        holder.setInitParameter("serviceInterface.RemoteMithraService", "com.gs.fw.common.mithra.remote.RemoteMithraService");
        holder.setInitParameter("serviceClass.RemoteMithraService", "com.gs.fw.common.mithra.remote.RemoteMithraServiceImpl");
        holder.setInitOrder(10);

        try
        {
            server.start();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not start server", e);
        }
        finally
        {
        }
    }

    public void workerVmSetUp()
    {
        String xmlFile = System.getProperty("mithra.xml.config");

        this.setDefaultServerTimezone();

        mithraTestResource = new MithraTestResource(xmlFile);

        mithraTestResource.setRestrictedClassList(this.getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        TimeZone databaseTimezone = this.getServerDatabaseTimezone();
        if(databaseTimezone != null)
        {
            connectionManager.setDatabaseTimeZone(databaseTimezone);
        }
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        try
        {
            mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "A", "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceA.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 0, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
            mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "B", "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceB.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 1, "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
            mithraTestResource.createSingleDatabase(connectionManager, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSource.txt");
            mithraTestResource.addTestDataForPureObjects(MITHRA_TEST_DATA_FILE_PATH + "mithraTestPure.txt");

            mithraTestResource.setUp();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not initialize mithra", e);
        }
    }

    protected void setDefaultServerTimezone()
    {
    }

    protected TimeZone getServerDatabaseTimezone()
    {
        return null;
    }

    public void workerVmTearDown()
    {
        mithraTestResource.tearDown();
    }

    protected Class[] getRestrictedClassList()
    {
        return null;
    }

    protected void addTestClassesFromOther(MithraTestAbstract otherTest, Set<Class> toAdd)
    {
        Class[] otherList = otherTest.getRestrictedClassList();
        if (otherList != null)
        {
            for(int i=0;i<otherList.length;i++)
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
