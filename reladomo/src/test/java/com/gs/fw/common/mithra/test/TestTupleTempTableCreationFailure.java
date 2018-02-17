

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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.test.domain.AccountTransaction;
import com.gs.fw.common.mithra.test.domain.AccountTransactionFinder;
import com.gs.fw.common.mithra.test.domain.AccountTransactionList;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.ParaDeskList;
import com.gs.fw.common.mithra.util.TrueFilter;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.utility.Iterate;

import java.sql.Timestamp;
import java.util.Collection;

public class TestTupleTempTableCreationFailure
extends TestSqlDatatypes
{
    @Override
    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
                ParaDesk.class,
                AccountTransaction.class
        };
    }

    @Override
    protected MithraTestResource buildMithraTestResource()
    {
        MithraTestResource mithraTestResource = super.buildMithraTestResource();
        mithraTestResource.setDatabaseType(H2DatabaseTypeForTests.getInstance());
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(); // each test should override this after setup is completed
        H2DatabaseTypeForTests.getInstance().setSimulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure(true);
        return mithraTestResource;
    }

    @Override
    protected void tearDown() throws Exception
    {
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence();
        H2DatabaseTypeForTests.getInstance().setSimulateSybaseConnectionDeadIndicatorsForTempTableCreationFailure(false);
        super.tearDown();
    }

    public void testListQueryOnSingleSource()
    {
        // Engineer the test so the first attempt fails, then the retry will succeed
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE);

        IntHashSet intHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            intHashSet.add(i);
        }
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.tagInt().in(intHashSet).and(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime()))));

        // Assert not only that the SQL execution does not fail but also that the in-clause is resolved correctly (upon retry) to retrieve the correct results
        assertEquals(4, list.size());
        Collection<String> deskIds = Iterate.collect(list, ParaDeskFinder.deskIdString());
        assertEquals(4, deskIds.size());
        assertTrue(deskIds.contains("swp"));
        assertTrue(deskIds.contains("gma"));
        assertTrue(deskIds.contains("gnb"));
        assertTrue(deskIds.contains("qqq"));
    }

    public void testCursorQueryOnSingleSource()
    {
        // Engineer the test so the first attempt fails, then the retry will succeed
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE);

        IntHashSet intHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            intHashSet.add(i);
        }
        Operation op = ParaDeskFinder.tagInt().in(intHashSet).and(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())));
        Cursor cursor = ParaDeskFinder.getMithraObjectPortal().findCursorFromServer(
                op,
                TrueFilter.instance(), // no post load filter
                null, // no ordering
                0,    // retrieve as many objects as necessary
                true, // read from the DB
                1,    // use single thread
                false);

        try
        {
            // Assert not only that the SQL execution does not fail but also that the in-clause is resolved correctly (upon retry) to retrieve the correct results

            FastList<String> deskIds = FastList.newList();
            while (cursor.hasNext())
            {
                ParaDesk desk = (ParaDesk) cursor.next();
                assertNotNull(desk);
                deskIds.add(desk.getDeskIdString());
            }

            assertEquals(4, deskIds.size());
            assertTrue(deskIds.contains("swp"));
            assertTrue(deskIds.contains("gma"));
            assertTrue(deskIds.contains("gnb"));
            assertTrue(deskIds.contains("qqq"));
        }
        finally
        {
            cursor.close();
        }
    }

    public void testListQueryFailureOnFirstSource()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the first query will fail, not the second
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE);

        runListQueryAcrossTwoSources();
    }

    public void testListQueryFailureOnSecondSource()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the second query will fail, not the first
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.FALSE, Boolean.TRUE);

        runListQueryAcrossTwoSources();
    }

    public void testListQueryFailureOnBothSources()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the first query will fail, retry will be successful, then the second query will fail
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE);

        runListQueryAcrossTwoSources();
    }

    private void runListQueryAcrossTwoSources()
    {
        IntHashSet intHashSet = new IntHashSet();
        for (int i=1;i>-1007;i--)
        {
            intHashSet.add(i);
        }
        Operation op = AccountTransactionFinder.transactionId().notIn(intHashSet).and(AccountTransactionFinder.deskId().in(UnifiedSet.newSetWith("A", "B")));
        AccountTransactionList list = new AccountTransactionList(op);

        // Assert not only that the SQL execution does not fail but also that the in-clause is resolved correctly (upon retry) to retrieve the correct results
        assertEquals(4, list.size());
        Collection<String> tranIds = Iterate.collect(list, new Function<AccountTransaction, String>()
        {
            @Override
            public String valueOf(AccountTransaction tran)
            {
                return tran.getDeskId() + ":" + tran.getTransactionId();
            }
        });
        assertEquals(4, tranIds.size());
        assertTrue(tranIds.contains("A:100"));
        assertTrue(tranIds.contains("A:1000"));
        assertTrue(tranIds.contains("B:10000"));
        assertTrue(tranIds.contains("B:100000"));
    }

    public void testCursorQueryFailureOnFirstSource()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the first query will fail, not the second
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE);

        runCursorQueryAcrossTwoSources();
    }

    public void testCursorQueryFailureOnSecondSource()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the second query will fail, not the first
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.FALSE, Boolean.TRUE);

        runCursorQueryAcrossTwoSources();
    }

    public void testCursorQueryFailureOnBothSources()
    {
        // The retrieval is split into two queries - one for each source
        // Engineer the test so that the first query will fail, retry will be successful, then the second query will fail
        H2DatabaseTypeForTests.getInstance().setTempTableCreationSuppressionSequence(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE);

        runCursorQueryAcrossTwoSources();
    }

    private void runCursorQueryAcrossTwoSources()
    {
        IntHashSet intHashSet = new IntHashSet();
        for (int i=1;i>-1007;i--)
        {
            intHashSet.add(i);
        }
        Operation op = AccountTransactionFinder.transactionId().notIn(intHashSet).and(AccountTransactionFinder.deskId().in(UnifiedSet.newSetWith("A", "B")));
        Cursor cursor = AccountTransactionFinder.getMithraObjectPortal().findCursorFromServer(
                op,
                TrueFilter.instance(), // no post load filter
                null, // no ordering
                0,    // retrieve as many objects as necessary
                true, // read from the DB
                1,    // use single thread
                false);

        try
        {
            // Assert not only that the SQL execution does not fail but also that the in-clause is resolved correctly (upon retry) to retrieve the correct results

            FastList<String> tranIds = FastList.newList();
            while (cursor.hasNext())
            {
                AccountTransaction tran = (AccountTransaction) cursor.next();
                assertNotNull(tran);
                tranIds.add(tran.getDeskId() + ":" + tran.getTransactionId());
            }

            assertEquals(4, tranIds.size());
            assertTrue(tranIds.contains("A:100"));
            assertTrue(tranIds.contains("A:1000"));
            assertTrue(tranIds.contains("B:10000"));
            assertTrue(tranIds.contains("B:100000"));
        }
        finally
        {
            cursor.close();
        }
    }
}
