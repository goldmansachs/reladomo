
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.databasetype.DerbyDatabaseType;
import junit.framework.TestCase;

import java.sql.*;
import java.util.*;

public class MithraTestAbstractUsingDerby extends TestCase
{
    private static final Logger logger = LoggerFactory.getLogger(MithraTestAbstractUsingDerby.class.getName());
    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";

    private MithraTestObjectToResultSetComparator mithraTestObjectToResultSetComparator;
    private MithraTestResource mithraTestResource;

    public MithraTestAbstractUsingDerby()
    {
        super("Mithra OrderBy Test Cases");
    }

    public MithraTestAbstractUsingDerby (String s)
    {
        super(s);
    }

    public MithraTestObjectToResultSetComparator getMithraTestObjectToResultSetComparator()
    {
        return mithraTestObjectToResultSetComparator;
    }

    public void setMithraTestObjectToResultSetComparator(MithraTestObjectToResultSetComparator mithraTestObjectToResultSetComparator)
    {
        this.mithraTestObjectToResultSetComparator = mithraTestObjectToResultSetComparator;
    }

    public static Logger getLogger()
    {
        return logger;
    }

    protected void setUp()
    throws Exception
    {
        MithraTestAbstract.setDerby(true);
        String xmlFile = System.getProperty("mithra.xml.config");

        mithraTestResource = new MithraTestResource(xmlFile, DerbyDatabaseType.getInstance());
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("C");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "C", "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceA.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 2, "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "D", "D", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceB.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 3, "D", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
        mithraTestResource.createSingleDatabase(connectionManager, "C", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSource.txt");

         mithraTestResource.setUp();
    }

    protected TimeZone getDatabaseTimeZone()
    {
        return TimeZone.getDefault();
    }

    protected void tearDown() throws Exception
    {
        MithraTestAbstract.setDerby(false);
        mithraTestResource.tearDown();
    }

    protected void orderedRetrievalTest(String directSql, List objectList)
            throws SQLException
    {
        this.orderedRetrievalTest(directSql, objectList, 1);
    }

    protected void orderedRetrievalTest (String directSql, List objectList, int minSize)
    throws SQLException
    {
        final Connection connection = this.getConnection();

        //Direct Sql results
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(directSql);

        //Direct Sql mapping
        List directList = new ArrayList();
        while (rs.next())
        {
            directList.add(this.createObjectFrom(rs));
        }

        this.matchOrderedRetrieval(directList, objectList);
        assertTrue("must have at least " + minSize + " results!", objectList.size() >= minSize);

        if (rs != null)
        {
            rs.close();
        }
        if (statement != null)
        {
            statement.close();
        }
        if (connection != null)
        {
            connection.close();
        }
    }

    protected void exactRetrievalTest(String directSql, List objectList, int exactSize)
    throws SQLException
    {
        final Connection connection = this.getConnection();

        //Direct Sql results
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(directSql);

        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have " + exactSize + " results!", objectList.size() == exactSize);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection)
            throws SQLException
    {
        this.genericRetrievalTest(statement, objectList, connection, 1);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection, int minSize)
            throws SQLException
    {
        ResultSet rs = statement.executeQuery();
        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have at least "+minSize+" results!", objectList.size() >= minSize);
    }

    protected void genericRetrievalTest(String directSql, List objectList)
            throws SQLException
    {
        this.genericRetrievalTest(directSql, objectList, 1);
    }

    protected void genericRetrievalTest(String directSql, List objectList, int minSize)
            throws SQLException
    {
        final Connection connection = this.getConnection();

        //Direct Sql results
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(directSql);

        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have at least "+minSize+" results!", objectList.size() >= minSize);
    }

    private void mapRetrievedObjects(final Connection connection, Statement statement, ResultSet rs, List objectList)
            throws SQLException
    {
        try
        {
            //Direct Sql mapping
            HashMap directMap = new HashMap();
            while (rs.next())
            {
                directMap.put(this.getPrimaryKeyFrom(rs), this.createObjectFrom(rs));
            }

            //Mithra Results
            Iterator objectIterator = objectList.iterator();

            //Mithra mapping
            HashMap mithraMap = new HashMap();
            while (objectIterator.hasNext())
            {
                Object object = objectIterator.next();
                mithraMap.put(this.getPrimaryKeyFrom(object), object);
            }

            //Matching
            this.matchRetrievals(directMap, mithraMap);
        }
        finally
        {
            if (rs != null) { rs.close(); }
            if (statement != null) { statement.close(); }
            if (connection != null) { connection.close(); }
        }
    }

    private void matchRetrievals(HashMap directMap, HashMap mithraMap)
    {
        //Matching
        assertEquals(directMap.size(), mithraMap.size());
        assertEquals(directMap.isEmpty(), mithraMap.isEmpty());

        Iterator directKeys = directMap.keySet().iterator();

        while (directKeys.hasNext())
        {
            Object directId = directKeys.next();
            assertEquals(directMap.get(directId), mithraMap.get(directId));
        }
    }

    private void matchOrderedRetrieval(List directList, List mithraList)
    {
        //Matching
        assertEquals(directList.size(), mithraList.size());
        assertEquals(directList.isEmpty(), mithraList.isEmpty());

        Iterator directIterator = directList.iterator();
        Iterator mithraIterator = mithraList.iterator();

        while (directIterator.hasNext())
        {
            assertEquals(directIterator.next(), mithraIterator.next());
        }
    }

    protected Object getPrimaryKeyFrom(Object mithraObject) throws SQLException
    {
        return this.mithraTestObjectToResultSetComparator.getPrimaryKeyFrom(mithraObject);
    }

    protected Object getPrimaryKeyFrom(ResultSet rs) throws SQLException
    {
        return this.mithraTestObjectToResultSetComparator.getPrimaryKeyFrom(rs);
    }

    protected Object createObjectFrom(ResultSet rs) throws SQLException
    {
        return this.mithraTestObjectToResultSetComparator.createObjectFrom(rs);
    }

    public static Connection getConnection ()
    {
        return ConnectionManagerForTests.getInstance().getConnection();
    }

    public Connection getConnection (String sourceId)
    {
        return ConnectionManagerForTests.getInstance().getConnection(sourceId);
    }

    public Connection getConnection (int sourceId)
    {
        return ConnectionManagerForTests.getInstance().getConnection(sourceId);
    }


    protected int getRetrievalCount()
    {
         return MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
    }

    protected Class[] getRestrictedClassList()
    {
        return null;
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
