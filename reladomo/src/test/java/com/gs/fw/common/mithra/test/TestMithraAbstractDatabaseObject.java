
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

import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.fw.common.mithra.MithraDatabaseException;
import com.gs.fw.common.mithra.connectionmanager.ConnectionManagerWrapper;
import com.gs.fw.common.mithra.database.*;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.test.domain.AuditedUserFinder;
import com.gs.fw.common.mithra.test.domain.AuditedUserList;
import com.gs.fw.common.mithra.test.domain.TestSwapPriceDatabaseObject;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.MithraRuntimeCacheController;
import com.mockobjects.sql.MockConnection2;

import java.sql.SQLException;
import java.util.ArrayList;

public class TestMithraAbstractDatabaseObject
        extends MithraTestAbstract
{
    private DatabaseType savedDatabaseType;

    private static class MockMithraStatsListener
            implements MithraStatsListener
    {
        private String preparedStatement;
        private String sql;
        private Class dbObjectClass;

        public void processRetrieval(Object source, PrintableStatementBuilder printableStatementBuilder, int rowsRetrieved, long queryStartTime, Class dbObjectClass)
        {
            this.preparedStatement = printableStatementBuilder.getStatementWithPlaceHolders();
            this.sql = printableStatementBuilder.getStatementWithActualParameters();
            this.dbObjectClass = dbObjectClass;
        }
    }

    private MockMithraStatsListener mockMithraStatsListener = new MockMithraStatsListener();

    @Override
    protected void setUp() throws Exception
    {
        MithraAbstractDatabaseObject.setStatsListenerFactory(new MithraStatsListenerFactory()
        {
            public MithraStatsListener createListener()
            {
                return mockMithraStatsListener;
            }
        });
        super.setUp();
        this.savedDatabaseType = ConnectionManagerForTests.getInstance().getDatabaseType();
        ConnectionManagerForTests.getInstance().setDatabaseType(SybaseDatabaseType.getInstance());
    }

    @Override
    protected void tearDown() throws Exception
    {
        ConnectionManagerForTests.getInstance().setDatabaseType(this.savedDatabaseType);
        MithraAbstractDatabaseObject.resetNoRetryThreadNames();
        MithraAbstractDatabaseObject.setStatsListenerFactory(null);
        super.tearDown();
    }

    public void testPerformanceListener()
    {
        if (new MithraRuntimeCacheController(AuditedUserFinder.class).isPartialCache())
        {
            new AuditedUserList(AuditedUserFinder.userId().eq("abc").and(AuditedUserFinder.sourceId().eq(1))).forceResolve();
            assertTrue(this.mockMithraStatsListener.preparedStatement.contains("?"));
            assertFalse(this.mockMithraStatsListener.preparedStatement.contains("abc"));
            assertFalse(this.mockMithraStatsListener.sql.contains("?"));
            assertTrue(this.mockMithraStatsListener.sql.contains("abc"));
            assertEquals("com.gs.fw.common.mithra.test.domain.AuditedUserDatabaseObject", this.mockMithraStatsListener.dbObjectClass.getName());
        }
    }
    public void testSqlLogSnooper()
    {
        if (new MithraRuntimeCacheController(AuditedUserFinder.class).isPartialCache())
        {
            SqlLogSnooper.startSqlSnooping();
            new AuditedUserList(AuditedUserFinder.userId().eq("abc").and(AuditedUserFinder.sourceId().eq(1))).forceResolve();
            String sql = SqlLogSnooper.completeSqlSnooping();
            assertTrue(sql.contains("AUDITED_USER_TBL"));
        }
    }

    public void testPerformanceListenerForCursor()
    {
        if (new MithraRuntimeCacheController(AuditedUserFinder.class).isPartialCache())
        {
            AuditedUserList auditedUserList = new AuditedUserList(AuditedUserFinder.userId().eq("abc").and(AuditedUserFinder.sourceId().eq(1)));
            auditedUserList.forEachWithCursor(new DoWhileProcedure()
            {
                public boolean execute(Object object)
                {
                    return false;
                }
            });

            assertTrue(this.mockMithraStatsListener.preparedStatement.contains("?"));
            assertFalse(this.mockMithraStatsListener.preparedStatement.contains("abc"));
            assertFalse(this.mockMithraStatsListener.sql.contains("?"));
            assertTrue(this.mockMithraStatsListener.sql.contains("abc"));
            assertEquals("com.gs.fw.common.mithra.test.domain.AuditedUserDatabaseObject", this.mockMithraStatsListener.dbObjectClass.getName());
        }
    }

    public void testKilledConnectionNonRetriableException()
    {
        MithraAbstractDatabaseObject.addNoRetryThreadNames(UnifiedSet.newSetWith(Thread.currentThread().getName()));
        SQLException killedConnectionException = this.createKilledConnectionException();
        this.executeTest(killedConnectionException, false);
        this.executeTest(killedConnectionException, true);
    }

    public void testKilledConnectionRetriableException()
    {
        SQLException killedConnectionException = this.createKilledConnectionException();
        this.executeTest(killedConnectionException, true);
        MithraAbstractDatabaseObject.addNoRetryThreadNames(UnifiedSet.newSetWith("ABC"));
        this.executeTest(killedConnectionException, true);
    }

    private SQLException createKilledConnectionException()
    {
        return new SQLException("com.sybase.jdbc4.jdbc.SybConnectionDeadException", SybaseDatabaseType.STATE_CONNECTION_CLOSED);
    }

    public void testDeadConnectionRetriableException()
    {
        this.executeTest(new SQLException("test", SybaseDatabaseType.STATE_CONNECTION_CLOSED, 0), true);
    }

    public void testDeadConnectionWithNonRetriableThread()
    {
        MithraAbstractDatabaseObject.addNoRetryThreadNames(UnifiedSet.newSetWith(Thread.currentThread().getName()));
        this.executeTest(new SQLException("test", SybaseDatabaseType.STATE_CONNECTION_CLOSED, 0), true);
    }


    private void executeTest(SQLException exception, boolean retriable)
    {
        try
        {
            TestSwapPriceDatabaseObject databaseObject = new TestSwapPriceDatabaseObject();
            ConnectionManagerForTests manager = ConnectionManagerForTests.getInstance();
            manager.setDatabaseType(SybaseDatabaseType.getInstance());
            databaseObject.setConnectionManager(manager, new ConnectionManagerWrapper(manager));
            databaseObject.analyzeAndWrapSqlExceptionGenericSource("test",
                    new ArrayList(),
                    exception,
                    null,
                    new MockConnection2());
            fail("Exception should've been thrown");
        }
        catch (MithraDatabaseException e)
        {
            assertEquals(retriable, e.isRetriable());
        }
    }
}