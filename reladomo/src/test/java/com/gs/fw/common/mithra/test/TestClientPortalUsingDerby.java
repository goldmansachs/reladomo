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

import com.gs.fw.common.mithra.databasetype.DerbyDatabaseType;
import com.gs.fw.common.mithra.test.domain.SpecialAccount;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;



public class TestClientPortalUsingDerby  extends RemoteMithraServerTestCase
{
    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";

    private MithraTestResource mithraTestResource;
    protected Class[] getRestrictedClassList()
    {
        Set<Class> result = new HashSet<Class>();
        addTestClassesFromOther(new TestOrderby(), result);
        result.add(SpecialAccount.class);
        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    protected void setUp() throws Exception
    {
        MithraTestAbstract.setDerby(true);
        super.setUp();
    }

    protected void tearDown() throws Exception
    {
        MithraTestAbstract.setDerby(false);
        super.tearDown();
    }

    public void slaveVmSetUp()
    {
        MithraTestAbstract.setDerby(true);
        String xmlFile = System.getProperty("mithra.xml.config");

        this.setDefaultServerTimezone();

        mithraTestResource = new MithraTestResource(xmlFile, DerbyDatabaseType.getInstance());

        mithraTestResource.setRestrictedClassList(this.getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("C");
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        try
        {
            mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "C", "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceA.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 0, "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
            mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "D", "D", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceB.txt");
            mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 1, "D", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
            mithraTestResource.createSingleDatabase(connectionManager, "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSource.txt");

            mithraTestResource.setUp();
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not initialize mithra", e);
        }
    }

    public void slaveVmTearDown()
    {
        MithraTestAbstract.setDerby(false);
        mithraTestResource.tearDown();
    }


    public void testDeepOrderByWithObjectInQuery() throws SQLException
    {
        TestOrderbyUsingDerby test = new TestOrderbyUsingDerby();
        test.testDeepOrderByWithObjectInQuery();
    }

    public void testDeepOrderByWithoutObjectInQuery() throws SQLException
    {
        TestOrderbyUsingDerby test = new TestOrderbyUsingDerby();
        test.testDeepOrderByWithoutObjectInQuery();
    }
}
