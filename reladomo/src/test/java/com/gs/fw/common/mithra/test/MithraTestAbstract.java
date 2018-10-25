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

import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.StringAttribute;
import com.gs.fw.common.mithra.connectionmanager.ObjectSourceConnectionManager;
import com.gs.fw.common.mithra.connectionmanager.SourcelessConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.OrderDriverFinder;
import com.gs.fw.common.mithra.test.util.Log4JRecordingAppender;
import junit.framework.TestCase;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Exchanger;

public abstract class MithraTestAbstract
        extends TestCase
{
    private static final Logger logger = LoggerFactory.getLogger(MithraTestAbstract.class.getName());
    protected static final String MITHRA_TEST_DATA_FILE_PATH = "testdata/";
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static final String SOURCE_A = "A";
    public static final String SOURCE_B = "B";

    private MithraTestObjectToResultSetComparator mithraTestObjectToResultSetComparator;
    private MithraTestResource mithraTestResource;
    private TestDatabaseTimeoutSetter databaseTimeoutSetter = null;

    private static boolean isDerby = false;


    public static void setDerby(boolean derby)
    {
        isDerby = derby;
    }

    public MithraTestAbstract()
    {
        super("Mithra Test Cases");
    }

    public MithraTestAbstract(String s)
    {
        super(s);
    }

    public void setDatabaseTimeoutSetter(TestDatabaseTimeoutSetter databaseTimeoutSetter)
    {
        this.databaseTimeoutSetter = databaseTimeoutSetter;
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

    protected MithraTestResource buildMithraTestResource()
    {
        String xmlFile = System.getProperty("mithra.xml.config");
        return new MithraTestResource(xmlFile);
    }

    protected void setUp() throws Exception
    {
        mithraTestResource = buildMithraTestResource();
        mithraTestResource.setRestrictedClassList(getRestrictedClassList());

        ConnectionManagerForTests lewConnectionManager = ConnectionManagerForTests.getInstance("lew");
        ConnectionManagerForTests paraConnectionManager  = ConnectionManagerForTests.getInstance("para");
        mithraTestResource.createSingleDatabase(lewConnectionManager, MITHRA_TEST_DATA_FILE_PATH + "lewDataSource.txt");
        mithraTestResource.createSingleDatabase(paraConnectionManager, MITHRA_TEST_DATA_FILE_PATH + "paraDataSource.txt");

        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        connectionManager.setDefaultSource("A");
        connectionManager.setDatabaseTimeZone(this.getDatabaseTimeZone());
        connectionManager.setDatabaseType(mithraTestResource.getDatabaseType());

        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "A", "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceA.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 0, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceA.txt");
        mithraTestResource.createDatabaseForStringSourceAttribute(connectionManager, "B", "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataStringSourceB.txt");
        mithraTestResource.createDatabaseForIntSourceAttribute(connectionManager, 1, "B", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataIntSourceB.txt");
        mithraTestResource.createSingleDatabase(connectionManager, "A", MITHRA_TEST_DATA_FILE_PATH + "mithraTestDataDefaultSource.txt");
        mithraTestResource.addTestDataForPureObjects(MITHRA_TEST_DATA_FILE_PATH + "mithraTestPure.txt");
        mithraTestResource.setTestConnectionsOnTearDown(true);
        mithraTestResource.setUp();
    }

    protected DatabaseType getDatabaseType()
    {
        return this.mithraTestResource.getDatabaseType();
    }

    protected TimeZone getDatabaseTimeZone()
    {
        return TimeZone.getTimeZone("Asia/Tokyo");
//        return TimeZone.getDefault();
    }

    protected void tearDown() throws Exception
    {
        if (mithraTestResource != null)
        {
            mithraTestResource.tearDown();
        }
    }

    protected void orderedRetrievalTest(String directSql, List objectList, MithraTestObjectToResultSetComparator objectToResultSetComparator)
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(objectToResultSetComparator);
        this.orderedRetrievalTest(directSql, objectList, 1);
    }

    protected void orderedRetrievalTest(String directSql, List objectList, int minSize)
            throws SQLException
    {
        forceInit(objectList);
        final Connection connection = getConnection();

        Statement statement;
        ResultSet rs;
        try
        {
            //Direct Sql results
            statement = connection.createStatement();
            rs = statement.executeQuery(directSql);

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
        }
        finally
        {
            if (connection != null)
            {
                connection.close();
            }
        }

    }

    private void forceInit(List objectList)
    {
        Operation op = ((MithraList) objectList).getOperation();
        if (op != null)
        {
            op.getResultObjectPortal();
            op.addDependentPortalsToSet(new Set()
            {
                public boolean add(Object o) {return false;}
                public boolean addAll(Collection c) {return false;}
                public void clear() {}
                public boolean contains(Object o) { return false; }
                public boolean containsAll(Collection c) { return false; }
                public boolean isEmpty() { return true; }
                public Iterator iterator() { return null; }
                public boolean remove(Object o) { return false; }
                public boolean removeAll(Collection c) { return false; }
                public boolean retainAll(Collection c) { return false; }
                public int size() { return 0; }
                public Object[] toArray() { return null; }
                public Object[] toArray(Object[] a) { return null; }
            });
        }
        else if (objectList.size() > 0)
        {
            MithraManagerProvider.getMithraManager().initializePortal(objectList.get(0).getClass().getName());
        }
    }

    protected void exactRetrievalTest(String directSql, List objectList, int exactSize)
            throws SQLException
    {
        forceInit(objectList);
        final Connection connection = getConnection();

        //Direct Sql results
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(directSql);

        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have " + exactSize + " results!", objectList.size() == exactSize);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection, boolean clearAfter)
            throws SQLException
    {
        this.genericRetrievalTest(statement, objectList, connection, 1, clearAfter);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection, int minSize, boolean clearAfter)
            throws SQLException
    {
        ResultSet rs = statement.executeQuery();
        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have at least " + minSize + " results!", objectList.size() >= minSize);
        if (clearAfter)
        {
            MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        }
    }

    protected void genericRetrievalTest(String directSql, List objectList, boolean clearAfter)
            throws SQLException
    {
        this.genericRetrievalTest(directSql, objectList, 1, clearAfter);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection)
            throws SQLException
    {
        this.genericRetrievalTest(statement, objectList, connection, 1, true);
    }

    protected void genericRetrievalTest(PreparedStatement statement, List objectList, Connection connection, int minSize)
            throws SQLException
    {
        genericRetrievalTest(statement, objectList, connection, minSize, true);
    }

    protected void genericRetrievalTest(String directSql, List objectList)
            throws SQLException
    {
        this.genericRetrievalTest(directSql, objectList, 1, true);
    }

    protected void genericRetrievalTest(String directSql, List objectList, int minSize)
            throws SQLException
    {
        genericRetrievalTest(directSql, objectList, minSize, true);
    }

    protected void genericRetrievalTest(String directSql, List objectList, int minSize, boolean clearAfter)
            throws SQLException
    {
        forceInit(objectList);
        final Connection connection = getConnection();

        //Direct Sql results
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(directSql);

        this.mapRetrievedObjects(connection, statement, rs, objectList);
        assertTrue("must have at least " + minSize + " results!", objectList.size() >= minSize);
        if (clearAfter)
        {
            MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        }
    }

    public void genericRetrievalTest(final List directSqlList, List mithraObjectList)
            throws SQLException
    {
        forceInit(mithraObjectList);
        HashMap directMap = mapRetrievedObjectsFromMithraList(directSqlList);
        HashMap mithraMap = mapRetrievedObjectsFromMithraList(mithraObjectList);

        this.matchRetrievals(directMap, mithraMap);
    }

    private HashMap mapRetrievedObjectsFromMithraList(List objectList)
            throws SQLException
    {
        HashMap map = new HashMap();
        for (int i = 0; i < objectList.size(); i++)
        {
            MithraObject obj = (MithraObject) objectList.get(i);
            map.put(this.getPrimaryKeyFrom(objectList.get(i)), obj);
        }
        return map;
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
            assertEquals(directMap.size(), objectList.size());
            //Matching
            this.matchRetrievals(directMap, mithraMap);
        }
        finally
        {
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

    public Connection getConnection()
    {
        return ConnectionManagerForTests.getInstance().getConnection();
    }

    public Connection getConnection(String sourceId)
    {
        return ConnectionManagerForTests.getInstance().getConnection(sourceId);
    }

    public Connection getConnection(int sourceId)
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

    public static void sleep(long millis)
    {
        long now = System.currentTimeMillis();
        long target = now + millis;
        while (now < target)
        {
            try
            {
                Thread.sleep(target - now);
            }
            catch (InterruptedException e)
            {
                fail("why were we interrupted?");
            }
            now = System.currentTimeMillis();
        }
    }

    protected static boolean runMultithreadedTest(Runnable listInserterThread1, Runnable listInserterThread2)
    {
        return MultiThreadingUtil.runMultithreadedTest(listInserterThread1, listInserterThread2);
    }

    protected void setLockTimeout(int timeoutInMillis) throws SQLException
    {
        if (this.databaseTimeoutSetter != null)
        {
            this.databaseTimeoutSetter.setDatabaseLockTimeout(timeoutInMillis);
        }
        else
        {
            Connection con = null;
            Connection con2 = null;
            try
            {
                con = setTimeoutOnConnection(timeoutInMillis);
                con2 = setTimeoutOnConnection(timeoutInMillis);
            }
            finally
            {
                if (con != null) con.close();
                if (con2 != null) con2.close();
            }
        }
    }

    private Connection setTimeoutOnConnection(int timeoutInMillis)
            throws SQLException
    {
        Connection con;
        con = getConnection();
        Statement stm = con.createStatement();
        stm.execute("SET LOCK_TIMEOUT " + timeoutInMillis);
        stm.close();
        return con;
    }

    protected Object waitForOtherThread(final Exchanger exchanger)
    {
        return waitForOtherThreadAndPassObject(exchanger, null);
    }

    protected Object waitForOtherThreadAndPassObject(final Exchanger exchanger, Object object)
    {
        Object result = null;
        try
        {
            getLogger().debug("waiting..");
            result = exchanger.exchange(object);
            getLogger().debug("done waiting");
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return result;
    }

    protected byte toByte(int i)
    {
        if (i > 127) i -= 256;

        return (byte) (i & 0xFF);
    }

    public static List<Operation> getOperationsForFullCacheLoad(StringAttribute deskId, AsOfAttribute first, AsOfAttribute second)
    {
        List<Operation> result = new ArrayList<Operation>(2);
        if (isDerby)
        {
            result.add(getOperationForDesk("C", deskId, first, second));
            result.add(getOperationForDesk("D", deskId, first, second));
        }
        else
        {
            result.add(getOperationForDesk(SOURCE_A, deskId, first, second));
            result.add(getOperationForDesk(SOURCE_B, deskId, first, second));
        }
        return result;
    }

    private static Operation getOperationForDesk(String desk, StringAttribute deskId, AsOfAttribute first, AsOfAttribute second)
    {
        Operation op = deskId.eq(desk);
        if (first != null) op = op.and(first.equalsEdgePoint());
        if (second != null) op = op.and(second.equalsEdgePoint());
        return op;
    }

    public org.apache.log4j.Logger getSqlLogger(Class c)
    {
        String name = c.getName();
        return LogManager.getLogger("com.gs.fw.common.mithra.sqllogs." + name.substring(name.lastIndexOf('.') + 1));
    }

    public Log4JRecordingAppender setupRecordingAppender(Class c)
    {
        org.apache.log4j.Logger sqlLogger = getSqlLogger(c);
        sqlLogger.setLevel(Level.DEBUG);
        Log4JRecordingAppender appender = new Log4JRecordingAppender();
        sqlLogger.addAppender(appender);
        return appender;
    }

    public void tearDownRecordingAppender(Class c)
    {
        org.apache.log4j.Logger sqlLogger = getSqlLogger(c);
        sqlLogger.setLevel(Level.INFO);
        ArrayList<Log4JRecordingAppender> toRemove = new ArrayList<Log4JRecordingAppender>();
        Enumeration appenders = sqlLogger.getAllAppenders();
        while (appenders.hasMoreElements())
        {
            Object o = appenders.nextElement();
            if (o instanceof Log4JRecordingAppender)
            {
                toRemove.add((Log4JRecordingAppender) o);
            }
        }
        for (Log4JRecordingAppender appender : toRemove)
        {
            sqlLogger.removeAppender(appender);
        }
    }

    protected Timestamp newTimestamp(String date)
    {
        try
        {
            return new Timestamp(timestampFormat.parse(date).getTime());
        }
        catch (ParseException e)
        {
            throw new RuntimeException("could not parse " + date, e);
        }
    }

    protected void executeStatement(String s)
            throws SQLException
    {
        Connection con = null;
        try
        {
            con = ConnectionManagerForTests.getInstance().getConnection();
            con.createStatement().execute(s);
        }
        finally
        {
            try
            {
                if (con != null) con.close();
            }
            catch (SQLException e)
            {
                // ignore
            }
        }
    }

    protected void executeTransactionWithTempContextThatRollbacks(final MithraTransactionalList list)
    {
        list.forceRefresh();
        Assert.assertTrue(list.notEmpty());

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                @Override
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    list.forceRefresh();
                    list.deleteAll();
                    Assert.assertTrue(list.isEmpty());
                    OrderDriverFinder.createTemporaryContext().destroy();
                    throw new RuntimeException();// trigger the rollback
                }
            });
        }
        catch(RuntimeException e)
        {
            list.forceRefresh();
            Assert.assertTrue(list.notEmpty());
            return;
        }

        Assert.fail();
    }

    protected void insertTestData(List<? extends MithraObject> mithraObjects)
    {
        this.mithraTestResource.insertTestData(mithraObjects);
    }

    protected void addTestDataFromFileToDatabase(String testDataFileName, SourcelessConnectionManager connectionManager)
    {
        this.mithraTestResource.addTestDataToDatabase(testDataFileName, connectionManager);
    }

    protected void addTestDataFromFileToDatabase(String testDataFileName, ObjectSourceConnectionManager connectionManager, String sourceAttribute)
    {
        this.mithraTestResource.addTestDataToDatabase(testDataFileName, connectionManager, sourceAttribute);
    }

    protected int dbCalls()
    {
        return MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
    }

    protected Date getDawnOfTime()
    {
        try
        {
            return timestampFormat.parse("1900-01-01 00:00:00");
        }
        catch (ParseException e)
        {
            //never happens
        }
        return null;
    }

    protected Date createReferenceBusinessDate(int year, int month, int day)
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.AM_PM, Calendar.AM);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    protected Date createParaBusinessDate(Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 18);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    protected Date createBusinessDate(int year, int month, int day)
    {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.AM_PM, Calendar.PM);
        cal.set(Calendar.HOUR_OF_DAY, 18);
        cal.set(Calendar.MINUTE, 30);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    protected Date addDays(Date d, int days)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);
        cal.add(Calendar.DATE, days);

        return cal.getTime();
    }

    public static void assertEqualsAndHashCode(Object expected, Object actual)
    {
        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
    }

    public static void copyFile(File src, File dest) throws IOException
    {
        FileInputStream in = null;
        FileOutputStream out = null;
        FileChannel srcChan = null;
        FileChannel destChan = null;
        try
        {
            in = new FileInputStream(src);
            srcChan = in.getChannel();
            out = new FileOutputStream(dest);
            destChan = out.getChannel();
            long soFar = 0;
            long end = srcChan.size();
            while (soFar < end)
            {
                soFar += destChan.transferFrom(srcChan, 0, 1024*1024);
                destChan.position(soFar);
            }
        }
        finally
        {
            closeClosable(srcChan);
            closeClosable(in);
            closeClosable(destChan);
            closeClosable(out);
        }
    }

    private static void closeClosable(Closeable closable) throws IOException
    {
        if (closable != null) closable.close();
    }

}
