
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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.DatedEntityDesc;
import com.gs.fw.common.mithra.test.domain.DatedEntityDescType;
import com.gs.fw.common.mithra.test.domain.dated.DatedTable;
import com.gs.fw.common.mithra.test.domain.dated.DatedTableFinder;
import com.gs.fw.common.mithra.test.domain.dated.DatedTableList;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedTable;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedTableFinder;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedTableList;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedWithBusinessDate;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedWithBusinessDateFinder;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedWithTimestampTable;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedWithTimestampTableFinder;
import com.gs.fw.common.mithra.test.domain.dated.NotDatedWithTimestampTableList;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrial;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialFinder;
import com.gs.fw.common.mithra.test.domain.dated.TestTamsMithraTrialList;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class TestDatedWithNotDatedJoin extends MithraTestAbstract
{
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public TestDatedWithNotDatedJoin()
    {
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            DatedTable.class,
            NotDatedTable.class,
            NotDatedWithTimestampTable.class,
            NotDatedWithBusinessDate.class,
            DatedEntityDesc.class,
            DatedEntityDescType.class,
            TestTamsMithraTrial.class
        };
    }
    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new DatedTableResultSetComparator());
    }

    public void testDatedNotDatedJoin()
    throws Exception
    {
        Operation op = DatedTableFinder.all();
        op = op.and(DatedTableFinder.notDatedTable().id().eq(1));
        DatedTableList dated = new DatedTableList(op);
        dated.deepFetch(DatedTableFinder.notDatedTable());

        assertTrue(dated.size() == 1);
    }

    public void testDatedNotDatedWithTimestampJoiningByTimestamp()
    throws Exception
    {
        Set trialCodes = new HashSet();
        trialCodes.add("1");
        trialCodes.add("2");
        trialCodes.add("3");
        Date asOfDate = timestampFormat.parse("2000-01-02 00:00:00.0");


        Operation datedOp = TestTamsMithraTrialFinder.trialId().in(trialCodes);
                datedOp = datedOp.and(TestTamsMithraTrialFinder.businessDate().eq(asOfDate));

        TestTamsMithraTrialList testTamsTrialList0 = new TestTamsMithraTrialList(datedOp);

        assertEquals(1, testTamsTrialList0.size());

        Operation notDatedOp = NotDatedWithTimestampTableFinder.signoffDate().eq(new Timestamp(asOfDate.getTime())).
                and(NotDatedWithTimestampTableFinder.id().in(trialCodes));

        NotDatedWithTimestampTableList notDatedTableList0 = new NotDatedWithTimestampTableList(notDatedOp);
        assertEquals(0, notDatedTableList0.size());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Date asOfDate = timestampFormat.parse("2000-01-02 00:00:00.0");
                NotDatedWithTimestampTable newObj = new NotDatedWithTimestampTable();
                newObj.setId("1");
                newObj.setSignoffDate(new Timestamp(asOfDate.getTime()));
                newObj.insert();
                return null;
            }
        });

        NotDatedWithTimestampTableList notDatedTableList1  = new NotDatedWithTimestampTableList(notDatedOp);
        assertEquals(1, notDatedTableList1.size());

        Operation joinOp = TestTamsMithraTrialFinder.trialId().in(trialCodes);
                    joinOp = joinOp.and(TestTamsMithraTrialFinder.businessDate().eq(asOfDate));
                    joinOp = joinOp.and(TestTamsMithraTrialFinder.trialId().eq(NotDatedWithTimestampTableFinder.id()));
                    joinOp = joinOp.and(NotDatedWithTimestampTableFinder.signoffDate().greaterThanEquals(new Timestamp(asOfDate.getTime())));

        TestTamsMithraTrialList testTamsTrialList1 = new TestTamsMithraTrialList(joinOp);
        testTamsTrialList1.setBypassCache(true);
        assertEquals(1, testTamsTrialList1.size());
    }

    public void testDeepFetchNonDatedToDated() throws Exception
    {
        Timestamp asOfDate = new Timestamp(timestampFormat.parse("2000-01-02 00:00:00.0").getTime());

        Operation op = DatedTableFinder.id().eq(3);
        DatedTableList list = new DatedTableList(op);
        list.deepFetch(DatedTableFinder.notDatedTable().datedTable(asOfDate));
        assertEquals(1, list.size());
        assertNotNull(list.getDatedTableAt(0).getNotDatedTable());
        assertNotNull(list.getDatedTableAt(0).getNotDatedTable().getDatedTable(asOfDate));
    }

    public void testExistsNonDatedToDated() throws Exception
    {
        Timestamp asOfDate = new Timestamp(timestampFormat.parse("2000-01-02 00:00:00.0").getTime());
        IntHashSet set = IntHashSet.newSetWith(new int[]{1,2,3});
        NotDatedTableList notDated = new NotDatedTableList(NotDatedTableFinder.id().in(set).and(NotDatedTableFinder.datedTable(asOfDate).exists()));
        notDated.setOrderBy(NotDatedTableFinder.id().ascendingOrderBy());
        assertEquals(2, notDated.size());
        assertEquals(1, notDated.get(0).getId());
        assertEquals(3, notDated.get(1).getId());
    }

    public void testDatedToNonDatedExistsWithAsOfOp() throws ParseException
    {
        NotDatedTableList notDated = new NotDatedTableList(NotDatedTableFinder.id().eq(1));
        notDated.forceResolve();
        DatedTableList dated = new DatedTableList(DatedTableFinder.id().eq(1));
        dated.forceResolve();

        Operation op = DatedTableFinder.notDatedTable().id().eq(1);
        op = op.and(DatedTableFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime())));
        op = op.and(DatedTableFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2003-05-01 00:00:00").getTime())));

        assertEquals(1, DatedTableFinder.findMany(op).size());
    }

    public void testNonDatedWithBusinessDateToDated() throws ParseException
    {
        int dbCount = this.getRetrievalCount();
        Operation op = NotDatedWithBusinessDateFinder.quantity().greaterThan(0.5);
        op = op.and(NotDatedWithBusinessDateFinder.datedTable().exists());
        op = op.and(NotDatedWithBusinessDateFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2009-01-01 00:00:00").getTime())));
        assertEquals(2, NotDatedWithBusinessDateFinder.findMany(op).size());
        if (!NotDatedWithBusinessDateFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(dbCount, this.getRetrievalCount());
        }
    }

    public void testNonDatedWithBusinessDateToDatedTwoDeep() throws ParseException
    {
        int dbCount = this.getRetrievalCount();
        Operation op = NotDatedWithBusinessDateFinder.quantity().greaterThan(0.5);
        op = op.and(NotDatedWithBusinessDateFinder.datedTable().datedEntityDescType().exists());
        op = op.and(NotDatedWithBusinessDateFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2009-01-01 00:00:00").getTime())));
        assertEquals(1, NotDatedWithBusinessDateFinder.findMany(op).size());
        if (!NotDatedWithBusinessDateFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(dbCount, this.getRetrievalCount());
        }
    }

    public void testNonDatedWithBusinessDateToDatedTwoDeepNoDate() throws ParseException
    {
        int dbCount = this.getRetrievalCount();
        Operation op = NotDatedWithBusinessDateFinder.quantity().greaterThan(0.5);
        op = op.and(NotDatedWithBusinessDateFinder.datedTable().datedEntityDescType().exists());
        assertEquals(2, NotDatedWithBusinessDateFinder.findMany(op).size());
        if (!NotDatedWithBusinessDateFinder.getMithraObjectPortal().isPartiallyCached())
        {
            assertEquals(dbCount, this.getRetrievalCount());
        }
    }

}
