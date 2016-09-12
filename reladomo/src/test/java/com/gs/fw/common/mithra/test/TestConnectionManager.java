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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.connectionmanager.ObjectPoolWithThreadAffinity;
import com.gs.fw.common.mithra.connectionmanager.PoolableObjectFactory;
import com.gs.fw.common.mithra.test.domain.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestConnectionManager extends MithraTestAbstract
{

    public TestConnectionManager(String s)
    {
        super(s);
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
           User.class
        };
    }

    public void testSameConnectionForSameTransaction() throws Exception
    {
        Connection dummyConnection = getConnection();
        dummyConnection.close();
        MithraTransaction mt = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Connection firstConnection = getConnection();
        firstConnection.close();
        Connection secondConnection = getConnection();
        assertSame(firstConnection, secondConnection);
        secondConnection.close();
        mt.commit();
    }

    public void testStatementPooling() throws Exception
    {
        Connection con = getConnection();
        PreparedStatement ps = con.prepareStatement("select * from USER_TBL where NAME = ?");
        ps.close();
        con.close();
        con = getConnection();
        PreparedStatement ps2 = con.prepareStatement("select * from USER_TBL where NAME = ?");
        ps2.close();
        con.close();
        assertSame(ps, ps2);
    }



    public void testSameTransactionNeedingMultipleConnections() throws Exception
    {
        MithraTransaction mt = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        Connection firstConnection = getConnection();
        Connection secondConnection = getConnection();
        assertNotSame(firstConnection, secondConnection);
        firstConnection.close();
        secondConnection.close();
        mt.commit();
    }

    public void testCommitWithTransaction() throws Exception
    {
        MithraTransaction mt = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        String testName = "test";
        Connection connection = getConnection();
        updateUserName(testName, connection);
        connection.close();
        mt.commit();
        assertEquals(getUserName(), testName);
        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
    }

    public void testCommitWithoutTransaction() throws Exception
    {
        assertNull(MithraManagerProvider.getMithraManager().getCurrentTransaction());
        String testName = "test";
        Connection connection = getConnection();
        updateUserName(testName, connection);
        connection.commit();
        connection.close();
        assertEquals(getUserName(), testName);
    }

    public void testTransactionRollback() throws Exception
    {
        MithraTransaction mt = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        String testName = "rollbackTest";
        Connection connection = getConnection();
        updateUserName(testName, connection);
        connection.close();
        mt.rollback();
        if(testName.equals(getUserName())) fail("rollback doesn't work");
    }

    private void updateUserName(String testName, Connection connection)
            throws SQLException
    {
        PreparedStatement pstmt = connection.prepareStatement("update user_tbl set name = ? where objectid = ?");
        pstmt.setString(1, testName);
        pstmt.setInt(2, 1);
        pstmt.execute();
    }


    public void testPoolRelease() throws Exception
    {
        for(int i=0;i<3;i++)
        {
            this.testCommitWithTransaction();
        }
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        assertEquals(connectionManager.getNumberOfActiveConnections() , 0);
        assertTrue(connectionManager.getNumberOfIdleConnection() > 0);
    }

    private String getUserName()
            throws SQLException
    {
        Connection connection = getConnection();
        PreparedStatement pstmt = connection.prepareStatement("select name from user_tbl where objectid = ?");
        pstmt.setInt(1,1);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        String name = rs.getString(1);
        connection.close();
        return name;
    }

    public void testMultipleThreadsInTransaction() throws SQLException
    {
        int maxThreads = 100;
        Thread[] threads = new Thread[maxThreads];
        final TestConnectionManager.SecondConnectionManager secondConnectionManager = new TestConnectionManager.SecondConnectionManager();
        secondConnectionManager.addConnectionManagerForSource("X");
        secondConnectionManager.setDefaultSource("X");

        final boolean[] allOk = new boolean[maxThreads];

        for(int i=0;i<maxThreads;i++)
        {
            allOk[i] = true;
            final int count = i;
            threads[i] = new Thread()
            {
                public void run()
                {
                    try
                    {
                        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                                new TransactionalCommand() {
                                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                                    {
                                        Connection connection = getConnection();
                                        Thread.sleep(((int)(Math.random()*1000)));
                                        connection.close();
                                        Connection con2 = getConnection();
                                        assertSame(connection, con2);
                                        Thread.sleep(((int)(Math.random()*1000)));
                                        con2.close();
    //                                    Connection con2 = secondConnectionManager.getConnection();
    //                                    Thread.sleep(((int)(Math.random()*1000)));
    //                                    con2.close();

                                        return null;
                                    }
                                }
                        );
                    }
                    catch (Throwable e)
                    {
                        getLogger().error("transaction failed", e);
                        allOk[count] = false;
                    }
                }
            };
        }

        for(int i=0;i<maxThreads;i++)
        {
            threads[i].start();
        }

        for(int i=0;i<maxThreads;i++)
        {
            try
            {
                threads[i].join();
                assertTrue(allOk[i]);
            }
            catch (InterruptedException e)
            {
                fail("should not be interrupted!");
            }
        }
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance();
        assertEquals(0, connectionManager.getNumberOfActiveConnections());
        assertTrue(connectionManager.getNumberOfIdleConnection() > 1);

    }

    public class SecondConnectionManager extends ConnectionManagerForTests
    {
        public SecondConnectionManager()
        {
            super();
        }

    }

    public void testObjectPoolWithBadFactory() throws Exception
    {
        ObjectPoolWithThreadAffinity objectPool = new ObjectPoolWithThreadAffinity(new BadFactory(), 10,
                1000, 5, 5, true, true, -1, -1, -1);
        borrowAndReturn(objectPool);
        borrowAndReturn(objectPool);
        borrowAndReturn(objectPool);
        borrowAndReturn(objectPool);
    }

    private void borrowAndReturn(ObjectPoolWithThreadAffinity objectPool)
            throws Exception
    {
        Object one = objectPool.borrowObject();
        Object two = objectPool.borrowObject();
        Object three = objectPool.borrowObject();
        Object four = objectPool.borrowObject();
        Object five = objectPool.borrowObject();
        Object six = null;
        try
        {
            six = objectPool.borrowObject();
        }
        catch (Exception e)
        {
            // ignore
        }
        Object seven = objectPool.borrowObject();
        Object eight = objectPool.borrowObject();
        Object nine = objectPool.borrowObject();
        Object ten = objectPool.borrowObject();

        objectPool.returnObject(one);
        objectPool.returnObject(two);
        objectPool.returnObject(three);
        objectPool.returnObject(four);
        objectPool.returnObject(five);
        if (six != null) objectPool.returnObject(six);
        objectPool.returnObject(seven);
        objectPool.returnObject(eight);
        objectPool.returnObject(nine);
        objectPool.returnObject(ten);
    }

    private static class BadFactory implements PoolableObjectFactory
    {
        private int count = 0;

        public void activateObject(Object o) throws Exception
        {
            Integer i = (Integer) o;
            if (i == 3)
            {
                throw new Exception("for testing bad activate");
            }
        }

        public void destroyObject(Object o) throws Exception
        {
            Integer i = (Integer) o;
            if (i == 3 || i == 4)
            {
                throw new Exception("for testing bad destroy");
            }
        }

        public Object makeObject(ObjectPoolWithThreadAffinity pool) throws Exception
        {
            if (count == 5)
            {
                count++;
                throw new Exception("for testing bad make");
            }
            return new Integer(count++);
        }

        public void passivateObject(Object o) throws Exception
        {
            Integer i = (Integer) o;
            if (i == 2)
            {
                throw new Exception("for testing bad passivate");
            }
        }

        public boolean validateObject(Object o)
        {
            Integer i = (Integer) o;
            if (i == 7)
            {
                throw new RuntimeException("for testing bad validateObject");
            }
            return true;
        }
    }

}
