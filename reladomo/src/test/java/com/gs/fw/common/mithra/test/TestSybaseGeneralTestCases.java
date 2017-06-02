
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

import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.gs.collections.impl.set.mutable.primitive.DoubleHashSet;
import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.attribute.TupleAttributeImpl;
import com.gs.fw.common.mithra.connectionmanager.XAConnectionManager;
import com.gs.fw.common.mithra.databasetype.DatabaseType;
import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.aggregate.TestStandardDeviation;
import com.gs.fw.common.mithra.test.aggregate.TestVariance;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.alarm.Alarm;
import com.gs.fw.common.mithra.test.domain.alarm.AlarmFinder;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.*;
import com.gs.fw.common.mithra.util.Time;
import junit.framework.Assert;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Exchanger;

public class TestSybaseGeneralTestCases extends MithraSybaseTestAbstract
{
    private static Timestamp BUSINESS_DATE = Timestamp.valueOf("2014-10-01 23:59:00.0");

    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private TestForceRefresh testForceRefresh = new TestForceRefresh();
    private TestIn testIn = new TestIn();

    public void tearDown() throws Exception
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(60);
       super.tearDown();
    }

    public void testToCheckRepeatReadIsDisabled() throws Exception
    {
        new TestSybaseConnectionSetupForTests().testToCheckRepeatReadIsDisabled();
    }

    public void testUpdateOneRowManyTimes()
            throws SQLException
    {
        final Order orderOne = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(orderOne);
        final Order orderTwo = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(orderTwo);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (int i = 0; i < 1000; i++)
                {
                    orderOne.setUserId(0);
                    orderTwo.setUserId(7);
                    orderOne.setUserId(1);
                    orderTwo.setUserId(6);
                }
                return null;
            }
        });
        assertEquals(orderOne.getUserId(), 1);
    }

    public void testDateYearRetrieval()
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.dateValue().year().eq(2007));
        assertEquals(10, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.add(2005);
        intSet.add(2007);
        Operation eq2 = AllTypesFinder.dateValue().year().in(intSet);
        AllTypesList one2 = AllTypesFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(10, size2);
    }

    public void testDateMonthRetrieval()
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.dateValue().month().eq(1));
        assertEquals(1, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesFinder.dateValue().month().in(intSet);
        AllTypesList one2 = AllTypesFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(4, size2);
    }

    public void testDateDayOfMonthRetrieval()
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.dateValue().dayOfMonth().eq(1));
        assertEquals(10, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesFinder.dateValue().dayOfMonth().in(intSet);
        AllTypesList one2 = AllTypesFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(10, size2);
    }

    public void testYearTimestampRetrieval() throws Exception
    {
        Operation eq = AllTypesFinder.timestampValue().year().eq(2007);
        AllTypesList many = AllTypesFinder.findMany(eq);
        int size = many.size();
        assertEquals(10, size);

        IntHashSet intSet = new IntHashSet();
        intSet.add(2005);
        intSet.add(2007);
        Operation eq2 = TimestampConversionFinder.timestampValueNone().year().in(intSet);
        TimestampConversionList many2 = TimestampConversionFinder.findMany(eq2);
        int size2 = many2.size();
        assertEquals(3, size2);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.add(2005);
        intSet2.add(2007);
        Operation eq3 = TimestampConversionFinder.timestampValueDB().year().in(intSet2);
        TimestampConversionList many3 = TimestampConversionFinder.findMany(eq3);
        try
        {
            int size3 = many3.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }

        IntHashSet intSet3 = new IntHashSet();
        intSet3.addAll(2005);
        intSet3.add(2007);
        Operation eq4 = TimestampConversionFinder.timestampValueUTC().year().in(intSet3);
        TimestampConversionList many4 = TimestampConversionFinder.findMany(eq4);
        try
        {
            int size4 = many4.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }
    }

    public void testMonthTimestampRetrieval() throws Exception
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.timestampValue().month().eq(1));
        assertEquals(10, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = TimestampConversionFinder.timestampValueNone().month().in(intSet);
        TimestampConversionList many2 = TimestampConversionFinder.findMany(eq2);
        int size2 = many2.size();
        assertEquals(3, size2);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.addAll(1, 2, 3, 4);
        Operation eq3 = TimestampConversionFinder.timestampValueDB().month().in(intSet2);
        TimestampConversionList many3 = TimestampConversionFinder.findMany(eq3);
        try
        {
            int size3 = many3.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }

        IntHashSet intSet3 = new IntHashSet();
        intSet3.addAll(1, 2, 3, 4);
        Operation eq4 = TimestampConversionFinder.timestampValueUTC().month().in(intSet3);
        TimestampConversionList many4 = TimestampConversionFinder.findMany(eq4);
        try
        {
            int size4 = many4.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }
    }

    public void testDayOfMonthTimestampRetrieval() throws Exception
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.timestampValue().dayOfMonth().eq(1));
        assertEquals(1, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesFinder.timestampValue().dayOfMonth().in(intSet);
        AllTypesList one2 = AllTypesFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(4, size2);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.addAll(1, 2, 3 ,4);
        Operation eq3 = TimestampConversionFinder.timestampValueDB().dayOfMonth().in(intSet2);
        TimestampConversionList many3 = TimestampConversionFinder.findMany(eq3);
        try
        {
            int size3 = many3.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }

        IntHashSet intSet3 = new IntHashSet();
        intSet3.addAll(1, 2, 3, 4);
        Operation eq4 = TimestampConversionFinder.timestampValueUTC().dayOfMonth().in(intSet3);
        TimestampConversionList many4 = TimestampConversionFinder.findMany(eq4);
        try
        {
            int size4 = many4.size();
            throw new Exception("should not get here");
        }
        catch (MithraBusinessException e)
        {
            //good
        }
    }

    public void testStandardDeviation() throws ParseException
    {
        new TestStandardDeviation().testStdDevDouble();
    }

    public void testStandardDeviationInt() throws ParseException
    {
        new TestStandardDeviation().testStdDevInt();
    }

    public void testStandardDeviationLong() throws ParseException
    {
        new TestStandardDeviation().testStdDevLong();
    }

    public void testStandardDeviationShort() throws ParseException
    {
        new TestStandardDeviation().testStdDevShort();
    }

    public void testStandardDeviationFloat() throws ParseException
    {
        new TestStandardDeviation().testStdDevFloat();
    }

    public void testStandardDeviationByte() throws ParseException
    {
        new TestStandardDeviation().testStdDevByte();
    }

    public void testStandardDeviationBigDecimal() throws ParseException
    {
        new TestStandardDeviation(0.05).testStdDevBigDecimal();
    }

    public void testStandardDeviationPopDouble() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopDouble();
    }

    public void testStandardDeviationPopInt() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopInt();
    }

    public void testStandardDeviationPopLong() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopLong();
    }

    public void testStandardDeviationPopShort() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopShort();
    }

    public void testStandardDeviationPopFloat() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopFloat();
    }

    public void testStandardDeviationPopByte() throws ParseException
    {
        new TestStandardDeviation().testStdDevPopByte();
    }

    public void testStandardDeviationPopBigDecimal() throws ParseException
    {
        new TestStandardDeviation(0.05).testStdDevPopBigDecimal();
    }

    public void testVarianceDouble() throws ParseException
    {
        new TestVariance().testVarianceDouble();
    }

    public void testVarianceInt() throws ParseException
    {
        new TestVariance().testVarianceInt();
    }

    public void testVarianceLong() throws ParseException
    {
        new TestVariance().testVarianceLong();
    }

    public void testVarianceShort() throws ParseException
    {
        new TestVariance().testVarianceShort();
    }

    public void testVarianceFloat() throws ParseException
    {
        new TestVariance().testVarianceFloat();
    }

    public void testVarianceByte() throws ParseException
    {
        new TestVariance().testVarianceByte();
    }

    public void testVarianceBigDecimal() throws ParseException
    {
        new TestVariance(0.05).testVarianceBigDecimal();
    }

    public void testVariancePopDouble() throws ParseException
    {
        new TestVariance().testVariancePopDouble();
    }

    public void testVariancePopInt() throws ParseException
    {
        new TestVariance().testVariancePopInt();
    }

    public void testVariancePopLong() throws ParseException
    {
        new TestVariance().testVariancePopLong();
    }

    public void testVariancePopShort() throws ParseException
    {
        new TestVariance().testVariancePopShort();
    }

    public void testVariancePopFloat() throws ParseException
    {
        new TestVariance().testVariancePopFloat();
    }

    public void testVariancePopByte() throws ParseException
    {
        new TestVariance().testVariancePopByte();
    }

    public void testVariancePopBigDecimal() throws ParseException
    {
        new TestVariance(0.5).testVariancePopBigDecimal();
    }

    public void testTimestampMonthDayOfMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTimestampTupleMonthDayOfMonthTinySet();
    }

    public void testTimestampYearDayOfMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTimestampTupleYearDayOfMonthTinySet();
    }

    public void testTimestampYearMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTimestampTupleYearMonthTinySet();
    }

    public void testDateMonthDayOfMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTupleMonthDayOfMonthTinySet();
    }

    public void testDateYearDayOfMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTupleYearDayOfMonthTinySet();
    }

    public void testDateYearMonthTuplesTiny()
    {
        new TestYearMonthTuple().testTupleYearMonthTinySet();
    }

    public void testDateYearMonthTuplesValueOf()
    {
        new TestYearMonthTuple().testValueOf();
    }

    public void testDateYearMonthTuples()
    {
        new TestYearMonthTuple().testTupleYearMonthSet();
    }

    public void testTimestampTuplesValueOf()
    {
        new TestYearMonthTuple().testTimestampValueOf();
    }

    public void testTimestampYearMonthTuples()
    {
        new TestYearMonthTuple().testTimestampTupleYearMonthSet();
    }

    public void testStringLikeEscapes()
    {
        new TestStringLike().testStringLikeEscapes();
    }

    public void testSimpleOrder()
    {
        Assert.assertEquals(4, new ProductList(ProductFinder.all()).size());
    }

    public void testTransactionalBasicTime()
    {
        new TestSybaseTimeTests().testTransactionalBasicTime();
    }

    public void testTimeTransactionalUpdate()
    {
        new TestSybaseTimeTests().testTimeTransactionalUpdate();
    }

    public void testTimeTransactionalToString()
    {
        new TestSybaseTimeTests().testTimeTransactionalToStringWithSybaseAseRounding();
    }

    public void testTimestampGranularity()
    {
        new TestSybaseTimeTests().testTimestampGranularityWithSybaseAseRounding();
    }

    public void testTimeGranularity()
    {
        new TestSybaseTimeTests().testTimeGranularityWithSybaseAseRounding();
    }

    public void testModifyTimePrecisionOnSet()
    {
        new TestSybaseTimeTests().testModifyTimePrecisionOnSet();
    }

    public void testTimeTransactionalInsert()
    {
        new TestTimeTransactional().testInsert();
    }

    public void testTimeTransactionalDelete()
    {
        // In ASE timestamps are stored with 1/300s precision. So 11ms is rounded to 10ms by ASE when the query is performed, and the row with 10ms is matched.
        // Perform an additional ASE-specific test to verify that this rounding on the query has taken place.
        Alarm alarm =  AlarmFinder.findOne(AlarmFinder.time().eq(Time.withMillis(10, 30, 59, 11)));
        assertEquals(Time.withMillis(10, 30, 59, 10), alarm.getTime());

        // Now delegate to the same test we run on other DBs
        new TestTimeTransactional().testDelete();
    }

    public void testTimeTransactionalNullTime()
    {
        new TestTimeTransactional().testNullTime();
    }

    public void testTimeNonTransactionalBasicTime()
    {
        new TestSybaseTimeTests().testTimeNonTransactionalBasicTime();
    }

    public void testTimeNonTransactionalToString()
    {
        new TestSybaseTimeTests().testTimeNonTransactionalToString();
    }

    public void testTimeNonTransactionalNullTime()
    {
        new TestTimeNonTransactional().testNullTime();
    }

    public void testTimeDatedTransactionalBasicTime()
    {
        new TestSybaseTimeTests().testTimeDatedTransactionalBasicTime();
    }

    public void testTimeDatedTransactionalUpdateTime()
    {
        new TestSybaseTimeTests().testTimeDatedTransactionalUpdate();
    }

    public void testTimeDatedTransactionalToString()
    {
        new TestSybaseTimeTests().testTimeDatedTransactionalToString();
    }

    public void testTimeDatedTransactionalInsert()
    {
        new TestTimeDatedTransactional().testInsert();
    }

    public void testTimeDatedTransactionalTerminate()
    {
        new TestTimeDatedTransactional().testTerminate();
    }

    public void testTimeDatedTransactionalNullTime()
    {
        new TestTimeDatedTransactional().testNullTime();
    }

    public void testTimeDatedNonTransactionalBasicTime()
    {
        new TestSybaseTimeTests().testTimeDatedNonTransactionalBasicTime();
    }

    public void testTimeDatedNonTransactionalToString()
    {
        new TestSybaseTimeTests().testTimeDatedNonTransactionalToString();
    }

    public void testTimeDatedNonTransactionalNullTime()
    {
        new TestTimeDatedNonTransactional().testNullTime();
    }

    public void testTimeBitemporalTransactionalUpdateUntilTime()
    {
        new TestSybaseTimeTests().testBitemporalUpdateUntil();
    }

    public void testTimeBitemporalTransactionalInsertUntilTime()
    {
        new TestSybaseTimeTests().testBitemporalInsertUntil();
    }

    public void testTimeTuples()
    {
        new TestTimeTuple().testTupleSet();
    }

    public void testRollback()
    {
        new CommonVendorTestCases().testRollback();
    }


    public void testRetrieveOneRow()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        String sql = "select * from ALL_TYPES where ID = "+id;
        validateMithraResult(op, sql);
    }

    public void testRetrieveOneRowUsingTimestampInOperation()
    throws ParseException
    {
        Operation op = AllTypesFinder.timestampValue().eq(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.999").getTime()));
        String sql = "select * from ALL_TYPES where TIMESTAMP_COL = '2007-01-01 01:01:01.999'";
        validateMithraResult(op, sql);
    }

    public void testRetrieveMultipleRows()
    throws ParseException
    {
        Operation op = AllTypesFinder.timestampValue().greaterThanEquals(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.999").getTime()));
        String sql = "select * from ALL_TYPES where TIMESTAMP_COL >= '2007-01-01 01:01:01.999'";
        validateMithraResult(op, sql);
    }

    public void testLargeInClause()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet);
        String sql = "select * from ALL_TYPES where ID >= "+initialId;
        validateMithraResult(op, sql);
    }

    public void testLargeInClauseInTransaction()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        final IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(5200, AllTypesFinder.findMany(AllTypesFinder.id().in(idSet)).size());
                return null;
            }
        });
    }

    public void testLargeInClauseInTransactionWithRollback()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        final IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            assertEquals(5200, AllTypesFinder.findMany(AllTypesFinder.id().in(idSet)).size());
                            throw new RuntimeException("for testing rollback");
                        }
                    }
            );
        }
        catch(RuntimeException e)
        {
            assertEquals("for testing rollback", e.getMessage());
        }
    }

    public void testLargeInClauseInTransactionWithRollbackMonsterQuery()
    {
        final int initialId = 10000;
        final int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        final IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            assertEquals(5200, AllTypesFinder.findMany(AllTypesFinder.id().in(idSet)).size());
                            Operation op = AllTypesFinder.id().eq(initialId);
                            for(int i = 0; i < setSize; i++)
                            {
                                op = op.or(AllTypesFinder.id().eq(initialId+i).and(AllTypesFinder.nullableIntValue().eq(2000000000)));
                            }
                            assertEquals(5200, AllTypesFinder.findManyBypassCache(op).size());
                            fail("shouldn't get here; the previous stmt should make the database throw an exception");
                            return null;
                        }
                    }
            );
        }
        catch(RuntimeException e)
        {
//            assertEquals("for testing rollback", e.getMessage());
        }
    }

     public void testLargeInClauseInParallel()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        AllTypesFinder.clearQueryCache();
        final Operation op = AllTypesFinder.id().in(idSet);
        final String sql = "select * from ALL_TYPES where ID >= "+initialId;


        AllTypesList list = new AllTypesList(op);
        list.setNumberOfParallelThreads(5);
        validateMithraResult(list, sql, 1);
    }

    public void testLargeInClauseBigAndSmall()
    {
        IntHashSet idSet = createIntHashSet(90);
        IntHashSet set2 = createIntHashSet(10);
        IntHashSet set3 = createIntHashSet(10);

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.intValue().in(set2).and(AllTypesFinder.id().in(idSet)).and(AllTypesFinder.nullableIntValue().in(set3));
        AllTypesFinder.findMany(op).forceResolve();

    }

    public void testLargeInClauseTwoSixty()
    {
        twoLargeInClause(60);
    }

    public void testLargeInClauseTwoOneTwenty()
    {
        twoLargeInClause(120);
    }

    public void testLargeInClauseThreeSixty()
    {
        threeLargeInClause(60);
    }

    public void testLargeInClauseThreeOneTwenty()
    {
        threeLargeInClause(120);
    }

    protected void twoLargeInClause(int setSize)
    {
        IntHashSet idSet = createIntHashSet(setSize);

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet).and(AllTypesFinder.intValue().in(idSet));
        AllTypesFinder.findMany(op).forceResolve();
    }

    private IntHashSet createIntHashSet(int setSize)
    {
        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(i);
        }
        return idSet;
    }

    protected void threeLargeInClause(int setSize)
    {
        IntHashSet idSet = createIntHashSet(setSize);

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet).and(AllTypesFinder.intValue().in(idSet)).and(AllTypesFinder.nullableIntValue().in(idSet));
        AllTypesFinder.findMany(op).forceResolve();
    }

    public void testLargeInClauseWithCursor()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        checkLargeInClause(initialId, setSize);
        checkLargeInClause(initialId, 1020); // exactly twice sybase batch size
        checkLargeInClause(initialId, 1021);
    }

    private void checkLargeInClause(int initialId, int setSize)
    {
        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet);
        AllTypesList list = new AllTypesList(op);
        final ArrayList resultsFromCursor = new ArrayList(setSize);
        list.forEachWithCursor(new DoWhileProcedure()
        {
            public boolean execute(Object object)
            {
                resultsFromCursor.add(object);
                return true;
            }
        });
        assertEquals(list.size(), resultsFromCursor.size());
    }

    public void testTwoLargeInClause()
    {
        int initialId = 10000;
        int setSize = 8000;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        IntHashSet otherSet = new IntHashSet(setSize);
        otherSet.add(2000000000);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
            otherSet.add(i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet).and(AllTypesFinder.intValue().in(otherSet));
        String sql = "select * from ALL_TYPES where ID >= "+initialId;
        validateMithraResult(op, sql);
    }

    public void testTwoLargeInClauseWithCursor()
    {
        int initialId = 10000;
        int setSize = 8000;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        IntHashSet otherSet = new IntHashSet(setSize);
        otherSet.add(2000000000);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
            otherSet.add(i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet).and(AllTypesFinder.intValue().in(otherSet));
        final AllTypesList list = new AllTypesList();
        new AllTypesList(op).forEachWithCursor(new DoWhileProcedure()
        {
            @Override
            public boolean execute(Object each)
            {
                list.add((AllTypes) each);
                return true;
            }
        });
//        String sql = "select * from ALL_TYPES where ID >= "+initialId;
//        validateMithraResult(op, sql);
    }

    public void testInsert()
    {
        int id = 9999999;
        AllTypes allTypesObj = this.createNewAllTypes(id, true);
        allTypesObj.insert();

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().eq(id);
        String sql = "select * from ALL_TYPES where ID = "+id;
        validateMithraResult(op, sql);
    }

    public void testInsertWithSpecialChars()
    {
        int id = 9999999;
        Order order = new Order();
        order.setOrderId(id);
        String weirdChar = new String(new char[]{(char) 196});
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("foo");
        order.setDescription(weirdChar);
        order.insert();

        order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(id));
        assertEquals(weirdChar, order.getDescription());
    }

    public void testInsertWithSpecialChars2()
    {
        int id = 9999999;
        Order order = new Order();
        order.setOrderId(id);
        String weirdChar = "test1\r\ntest2\r\ntest3";
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("foo");
        order.setDescription(weirdChar);
        order.insert();

        order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(id));
        assertEquals(weirdChar, order.getDescription());
    }

    public void testInsertCharWithSpecialChars()
    {
        int id = 9999999;
        AllTypes allTypesObj = this.createNewAllTypes(id, true);
        allTypesObj.setCharValue((char) 196);
        allTypesObj.setNullableCharValue((char) 196);
        allTypesObj.insert();

        allTypesObj = AllTypesFinder.findOneBypassCache(AllTypesFinder.id().eq(id));
        assertEquals(196, allTypesObj.getCharValue());
        assertEquals(196, allTypesObj.getNullableCharValue());
    }

    //todo: add tests for numeric types. Examples below have a scale of 2.
    public void testInsertFloatsAndDoublesWithRoundingIssues()
    {
        int id = 9999999;
        AllTypes allTypesObj = this.createNewAllTypes(id, true);
        float floatOne = Float.parseFloat("270532.60");
        double doubleOne = Double.parseDouble("270532.60");
        float floatTwo = Float.parseFloat("-98102.37");
        double doubleTwo = Double.parseDouble("-98102.37");
        allTypesObj.setFloatValue(floatOne);
        allTypesObj.setDoubleValue(doubleOne);
        allTypesObj.setNullableFloatValue(floatTwo);
        allTypesObj.setNullableDoubleValue(doubleTwo);
        allTypesObj.insert();

        long convertedOne = (long)((doubleOne * (Math.pow(10, 2)) + 0.5));
        long convertedTwo = Math.round(doubleTwo * (Math.pow(10, 2)));

        allTypesObj = AllTypesFinder.findOneBypassCache(AllTypesFinder.id().eq(id));
    }

    //todo: fix SYBIMAGE
    public void xtestBulkInsertFloatsAndDoublesWithRoundingIssues()
    {
        int id = 10000;
        AllTypesList list = new AllTypesList();
        for(int i=0;i<1000;i++)
        {
            AllTypes allTypesObj = this.createNewAllTypes(id, true);
            float floatOne = Float.parseFloat("270532.60");
            double doubleOne = Double.parseDouble("270532.60");
            float floatTwo = Float.parseFloat("145626.83");
            double doubleTwo = Double.parseDouble("145626.83");
            allTypesObj.setFloatValue(floatOne);
            allTypesObj.setDoubleValue(doubleOne);
            allTypesObj.setNullableFloatValue(floatTwo);
            allTypesObj.setNullableDoubleValue(doubleTwo);
            list.add(allTypesObj);
            id++;
        }
        list.bulkInsertAll();
    }

    public void testSubstring()
    {
        assertEquals(0, new OrderList(OrderFinder.description().eq("First")).size());
        OrderList orderList = new OrderList(OrderFinder.description().substring(0, 5).eq("First"));
        assertEquals(1, orderList.size());
        Order order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());

        assertEquals(0, new OrderList(OrderFinder.description().eq("first")).size());
        orderList = new OrderList(OrderFinder.description().substring(0, 5).toLowerCase().eq("first"));
        assertEquals(1, orderList.size());
        order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());

        assertEquals(0, new OrderList(OrderFinder.description().eq("irst order")).size());
        orderList = new OrderList(OrderFinder.description().substring(1, -1).eq("irst order"));
        assertEquals(1, orderList.size());
        order = orderList.getOrderAt(0);
        assertEquals(1, order.getOrderId());
    }

    public void testBulkInsertWithSpecialChars()
    {
        int id = 10000;
        OrderList list = new OrderList();
        String weirdChar = new String(new char[]{'a',(char) 196});
        for(int i=0;i<1000;i++)
        {
            Order order = new Order();
            order.setOrderId(id+i);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setState("foo");
            order.setDescription(weirdChar);
            list.add(order);
        }
        list.bulkInsertAll();

        Order order = OrderFinder.findOneBypassCache(OrderFinder.orderId().eq(id));
        assertEquals(weirdChar, order.getDescription());
    }

    public void testBulkInsertWithBigDecimal()
    {

        int id = 10000;
        BigOrderList bigOrderList = new BigOrderList();
        for(int i = 0; i < 1000; i++)
        {
            BigOrder order = new BigOrder();
            order.setOrderId(id + i);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setDescription("foo");
            order.setUserId(1);
            order.setDiscountPercentage(new BigDecimal(0.123+(i/10000.0)));
            bigOrderList.add(order);
        }
        bigOrderList.bulkInsertAll();
        BigOrder order = BigOrderFinder.findOneBypassCache(BigOrderFinder.orderId().eq(10001));
        assertEquals(BigDecimal.valueOf(0.123), order.getDiscountPercentage());
    }

    public void testBulkInsertWithReallyBigDecimal()
    {
        int id = 10000;
        final BigOrderItemList bigOrderItemList = new BigOrderItemList();
        for(int i = 0; i < 1000; i++)
        {
            int mod = i%2;


            BigOrderItem item = new BigOrderItem();
            item.setId(BigDecimal.valueOf(id + i));
            item.setOrderId(1);
            item.setProductId(1);
            if(mod == 0)
            {
                item.setQuantity(new BigDecimal("9999999.999"));
                item.setPrice(new BigDecimal("99999.99"));
                item.setBigPrice(new BigDecimal("999999999999999999.99999999999999999999"));
            }
            else
            {
                item.setQuantity(new BigDecimal("-9999999.999"));
                item.setPrice(new BigDecimal("-99999.99"));
                item.setBigPrice(new BigDecimal("-999999999999999999.99999999999999999999"));
            }
            bigOrderItemList.add(item);
        }
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                bigOrderItemList.insertAll();
                tx.setBulkInsertThreshold(1);
                return null;
            }
        });

        BigOrderItem item = BigOrderItemFinder.findOneBypassCache(BigOrderItemFinder.id().eq(10000));
        assertEquals(new BigDecimal("9999999.999"), item.getQuantity());
        assertEquals(new BigDecimal("99999.99"), item.getPrice());
        assertEquals(new BigDecimal("999999999999999999.99999999999999999999"), item.getBigPrice());

        BigOrderItem item2 = BigOrderItemFinder.findOneBypassCache(BigOrderItemFinder.id().eq(10001));
        assertEquals(new BigDecimal("-9999999.999"), item2.getQuantity());
        assertEquals(new BigDecimal("-99999.99"), item2.getPrice());
        assertEquals(new BigDecimal("-999999999999999999.99999999999999999999"), item2.getBigPrice());
    }

    public void testBigDecimalInsert()
    {
        BigDecimal discount = new BigDecimal("0.990");
        BigOrder order = this.createBigOrder(999, discount);
        order.insert();
        BigOrderFinder.clearQueryCache();
        BigOrder order2 = findOrder(999);
        assertEquals(discount, order2.getDiscountPercentage());
    }

    public void testBigDecimalUpdate()
    {
        final BigOrder order2 = findOrder(100);
        final BigDecimal discount = new BigDecimal("0.980");
        assertEquals(new BigDecimal("0.010"), order2.getDiscountPercentage());
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order2.setUserId(2);

                order2.setDiscountPercentage(discount);
                return null;
            }
        });

        BigOrder order3 = findOrder(100);
        assertEquals(discount, order3.getDiscountPercentage());

    }

    private BigOrder findOrder(int orderId)
    {
        return BigOrderFinder.findOne(BigOrderFinder.orderId().eq(orderId));
    }

    private BigOrder createBigOrder(int orderId, BigDecimal discount)
    {
        BigOrder order = new BigOrder();
        order.setOrderId(orderId);
        order.setDescription("Order"+orderId);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setUserId(1);
        order.setDiscountPercentage(discount);
        return order;
    }

    public void testForceRefreshBitemporalMultiPkWithBigDecimalList() throws SQLException
    {
        int countToInsert = 2034;
        MultiPkBigDecimalList list = new MultiPkBigDecimalList();
        for(int i=0;i<countToInsert;i++)
        {
            MultiPkBigDecimal testBal = new MultiPkBigDecimal();
            testBal.setId(777+i);
            testBal.setBdId(BigDecimal.valueOf(10+i));
            testBal.setQuantity(i*10);
            list.add(testBal);
        }
        list.insertAll();

        MultiPkBigDecimalList list2 = new MultiPkBigDecimalList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count+1, this.getRetrievalCount());
    }

    public void testBatchInsert()
    {
        int initialId = 10000;
        int setSize = 10;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();
        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().greaterThanEquals(initialId);
        String sql = "select * from ALL_TYPES where ID >= "+initialId;
        validateMithraResult(op, sql);
    }

    public void testBulkInsert()
    {
        final int initialId = 10000;
        final int listSize = 20;

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        ProductList productList = createNewProductList(initialId, listSize);
                        productList.insertAll();
                        tx.setBulkInsertThreshold(10);
                        return null;
                    }
                }
        );
        Operation op = ProductFinder.productId().greaterThanEquals(initialId);
        ProductList list = new ProductList(op);
        assertEquals(listSize, list.size());
    }

    public void testBatchInsertUpdate()
    {
        final int initialId = 10000;
        final int listSize = 2000;

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        ProductList productList = createNewProductList(initialId, listSize);
                        productList.insertAll();
                        ProductList updateList = ProductFinder.findManyBypassCache(ProductFinder.productId().greaterThanEquals(initialId + listSize/2));
                        System.out.println("updateList size = " + updateList.size());

                        for (Product each : updateList)
                        {
                            each.setManufacturerId(7);
                        }
                        return null;
                    }
                }
        );
        Operation op = ProductFinder.productId().greaterThanEquals(initialId);
        ProductList list = new ProductList(op);
        assertEquals(listSize, list.size());
    }

    public void testBulkInsertRollback()
    {
        final int initialId = 10000;
        final int listSize = 20;

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            ProductList productList = createNewProductList(initialId, listSize);
                            productList.insertAll();
                            tx.setBulkInsertThreshold(10);
                            ProductList everything = ProductFinder.findMany(ProductFinder.all());
                            if (everything.size() < productList.size())
                            {
                                fail("too few products");
                            }
                            throw new RuntimeException("for testing rollback");
                        }
                    }
            );
        }
        catch(RuntimeException e)
        {
            assertEquals("for testing rollback", e.getMessage());
        }
    }

    private void timeLargeIn(int size, String type)
    {
        IntHashSet set = new IntHashSet(size);
        for(int i=0;i<size;i++)
        {
            set.add(i+10000);
        }
        Operation op = ProductFinder.productId().in(set);
        timeOperation(op, type+" with "+size+" params");
    }

    private void timeOperation(Operation op, String msg)
    {
        for(int i=0;i<5;i++)
        {
            ProductFinder.clearQueryCache();
            ProductList list = new ProductList(op);
            long now = System.currentTimeMillis();
            list.forceResolve();
            System.out.println(msg+": took "+(System.currentTimeMillis() - now)+" ms");
        }
    }

    public void testSingleUpdate()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);
        allTypes.setIntValue(100);
        String sql = "select * from ALL_TYPES where ID = "+9876;
        validateMithraResult(op, sql);
    }

    public void testMultipleUpdatesToSameObjectWithNoTransaction()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);
        allTypes.setIntValue(100);
        allTypes.setNullableBooleanValue(true);
        allTypes.setNullableByteValue((byte)10);
        allTypes.setNullableCharValue('a');
        allTypes.setNullableShortValue((short)1000);
        allTypes.setNullableIntValue(987654);
        String sql = "select * from ALL_TYPES where ID = "+9876;
        validateMithraResult(op, sql);
    }

    public void testMultipleUpdatesToSameObjectInTransaction()
    {
        int id = 9876;
        final Operation op = AllTypesFinder.id().eq(id);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
             new TransactionalCommand()
             {
                 public Object executeTransaction(MithraTransaction tx) throws Throwable
                 {
                    AllTypes allTypes = AllTypesFinder.findOne(op);
                    allTypes.setIntValue(100);
                    allTypes.setNullableBooleanValue(true);
                    allTypes.setNullableByteValue((byte)10);
                    allTypes.setNullableCharValue('a');
                    allTypes.setNullableShortValue((short)1000);
                    allTypes.setNullableIntValue(987654);
                    return null;
                 }
             }
        );
        String sql = "select * from ALL_TYPES where ID = "+9876;
        validateMithraResult(op, sql);
    }

    public void x_testBatchUpdatePerRecordPerformance()
    {
        int initialId = 10000;
        final int setSize = 1000;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();
        final Operation op = AllTypesFinder.id().greaterThanEquals(9876);

        for (int j = 10; j<setSize; j+=10)
        {
            long start = System.currentTimeMillis();
            final int testCount = 10;
            for (int k = 0; k < testCount; k++)
            {
                final int K = k;
                final int J = j;
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                        new TransactionalCommand()
                        {
                            public Object executeTransaction(MithraTransaction tx) throws Throwable
                            {
                                AllTypesList allTypesList = new AllTypesList();
                                for (int i=0; i<J; i++) allTypesList.add(new AllTypesList(op).get(i));

                                for (int i = 0; i < allTypesList.size(); i++)
                                {
                                    AllTypes allTypes = allTypesList.get(i);
                                    allTypes.setIntValue(i + K + J * setSize);
                                }
                                tx.executeBufferedOperations();
                                return null;
                            }
                        }
                        , new TransactionStyle(120, 5, true)
                );
            }
            double ms = System.currentTimeMillis() - start;
            ms /= (double)(testCount * j);
            System.out.println("count=" + j + " took " + ms + " ms/rec");
        }
    }

    public void testBatchUpdate()
    {
        int initialId = 10000;
        int setSize = 1000;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();
        final Operation op = AllTypesFinder.id().greaterThanEquals(9876);

        final int[] retry = new int[1];
        for(int k=0;k<3;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         AllTypesList allTypesList = new AllTypesList(op);
                         for(int i = 0; i < allTypesList.size(); i++)
                         {
                             AllTypes allTypes  = allTypesList.get(i);
                             if (retry[0] <= 1)
                             {
                                 assertEquals(2000000000, allTypes.getIntValue());
                             }
                             allTypes.setIntValue(i+count);
                         }
                         if (retry[0] == 0)
                         {
                             tx.executeBufferedOperations();
                             sleep(10000);
                         }
                         retry[0]++;
                         return null;
                     }
                 }
            , new TransactionStyle(retry[0] == 0 ? 8 : 120, 5, true));
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        String sql = "select * from ALL_TYPES where ID >= "+9876;
        validateMithraResult(op, sql);
    }

    public void testBatchInsertAndUpdate()
    {
        final int initialId = 10000;
        final int setSize = 1000;

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        final AllTypesList insertList = createNewAllTypesList(initialId, setSize);

                        insertList.insertAll();


                        final Operation op = AllTypesFinder.id().greaterThanEquals(9876);
                        AllTypesList updateList = new AllTypesList(op);
                        for (int i = 0; i < updateList.size(); i++)
                        {
                            AllTypes allTypes = updateList.get(i);
                            assertEquals(2000000000, allTypes.getIntValue());
                            allTypes.setIntValue(i + 7);
                        }

                        return null;
                    }
                });

        String sql = "select * from ALL_TYPES where ID >= "+9876;
        validateMithraResult(AllTypesFinder.id().greaterThanEquals(9876), sql);
    }

    public void testUpdateOne_RowManyTimes() throws SQLException
    {
        final Order orderOne = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        assertNotNull(orderOne);
        final Order orderTwo = OrderFinder.findOne(OrderFinder.orderId().eq(2));
        assertNotNull(orderTwo);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                for (int i = 0; i < 1000; i++)
                {
                    orderOne.setUserId(0);
                    orderTwo.setUserId(7);
                    orderOne.setUserId(1);
                    orderTwo.setUserId(6);
                }
                return null;
            }
        });
        assertEquals(orderOne.getUserId(), 1);
    }

    public void testUpdateViaInsertDisabled()
    {
        this.runTestUpdatesViaInsert(-1);
    }

    public void testUpdateViaInsertZeroThreshold()
    {
        this.runTestUpdatesViaInsert(0);
    }

    public void testUpdateViaInsertSmallThreshold()
    {
        this.runTestUpdatesViaInsert(1);
    }

    public void testUpdateViaInsertLargeThreshold()
    {
        this.runTestUpdatesViaInsert(100);
    }

    public void runTestUpdatesViaInsert(int threshold)
    {
        int originalValue = this.getDatabaseType().getUpdateViaInsertAndJoinThreshold();
        try
        {
            this.getDatabaseType().setUpdateViaInsertAndJoinThreshold(threshold);

            this.runTestUpdatesViaInsert();
        }
        finally
        {
            this.getDatabaseType().setUpdateViaInsertAndJoinThreshold(originalValue);
        }
    }

    public void runTestUpdatesViaInsert()
    {
        AllTypesList allTypes = AllTypesFinder.findMany(AllTypesFinder.all());
        allTypes.setOrderBy(AllTypesFinder.id().descendingOrderBy());

        assertTrue(allTypes.size() > 0);

        final AllTypes maxId = allTypes.get(0).getNonPersistentCopy();

        AllTypesList newAllTypesList = new AllTypesList();
        for (int i = 1; i <= 1000; i++)
        {
            AllTypes newAllTypes = new AllTypes();
            newAllTypes.setId(maxId.getId() + i);
            newAllTypes.copyNonPrimaryKeyAttributesFrom(maxId);

            newAllTypesList.add(newAllTypes);
        }

        newAllTypesList.insertAll();

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest");

                return null;
            }
        });

        AllTypesList allTypesList1 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("MultiUpdateTest"));

        validateMithraResult(
                allTypesList1,
                "select * from ALL_TYPES where STRING_COL = 'MultiUpdateTest'",
                1000);
        assertEquals(1000, allTypesList1.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));

                many.setNullableStringValue("MultiUpdateTestNullable");

                return null;
            }
        });

        AllTypesList allTypesList2 = AllTypesFinder.findMany(AllTypesFinder.nullableStringValue().eq("MultiUpdateTestNullable"));
        validateMithraResult(
                allTypesList2,
                "select * from ALL_TYPES where NULL_STRING_COL = 'MultiUpdateTestNullable'",
                1000);
        assertEquals(1000, allTypesList2.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest2");
                many.setNullableStringValue("MultiUpdateTestNullable2");

                return null;
            }
        });

        AllTypesList allTypesList3 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("MultiUpdateTest2").and(AllTypesFinder.nullableStringValue().eq("MultiUpdateTestNullable2")));
        validateMithraResult(
                allTypesList3,
                "select * from ALL_TYPES where STRING_COL = 'MultiUpdateTest2' AND NULL_STRING_COL = 'MultiUpdateTestNullable2'",
                1000);
        assertEquals(1000, allTypesList3.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest2");
                many.setNullableStringValue("MultiUpdateTest2");

                return null;
            }
        });

        AllTypesList allTypesList4 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("MultiUpdateTest2").and(AllTypesFinder.nullableStringValue().eq("MultiUpdateTest2")));
        validateMithraResult(
                allTypesList4,
                "select * from ALL_TYPES where STRING_COL = 'MultiUpdateTest2' AND NULL_STRING_COL = 'MultiUpdateTest2'",
                1000);
        assertEquals(1000, allTypesList4.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest3");
                many.setNullableStringValue("MultiUpdateNullableTest3");
                many.setStringValue("MultiUpdateTest4");
                many.setNullableStringValue("MultiUpdateNullableTest4");

                return null;
            }
        });

        AllTypesList allTypesList5 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("MultiUpdateTest4").and(AllTypesFinder.nullableStringValue().eq("MultiUpdateNullableTest4")));
        validateMithraResult(
                allTypesList5,
                "select * from ALL_TYPES where STRING_COL = 'MultiUpdateTest4' AND NULL_STRING_COL = 'MultiUpdateNullableTest4'",
                1000);
        assertEquals(1000, allTypesList5.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest");
                    each.setNullableStringValue("BatchUpdateTestNullable");
                }

                return null;
            }
        });

        AllTypesList allTypesList6 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("BatchUpdateTest").and(AllTypesFinder.nullableStringValue().eq("BatchUpdateTestNullable")));
        validateMithraResult(
                allTypesList6,
                "select * from ALL_TYPES where STRING_COL = 'BatchUpdateTest' AND NULL_STRING_COL = 'BatchUpdateTestNullable'",
                1000);
        assertEquals(1000, allTypesList6.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest2");
                    each.setNullableStringValue("BatchUpdateTestNullable2");
                }

                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest3");
                }

                for (AllTypes each : many)
                {
                    each.setNullableStringValue("BatchUpdateTestNullable3");
                }

                return null;
            }
        });

        AllTypesList allTypesList7 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("BatchUpdateTest3").and(AllTypesFinder.nullableStringValue().eq("BatchUpdateTestNullable3")));
        validateMithraResult(
                allTypesList7,
                "select * from ALL_TYPES where STRING_COL = 'BatchUpdateTest3' AND NULL_STRING_COL = 'BatchUpdateTestNullable3'",
                1000);
        assertEquals(1000, allTypesList7.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest4");
                    each.setNullableStringValue("BatchUpdateTestNullable4");
                }

                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest5");
                    each.setNullableStringValue("BatchUpdateTestNullable5");
                }

                return null;
            }
        });

        AllTypesList allTypesList8 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("BatchUpdateTest5").and(AllTypesFinder.nullableStringValue().eq("BatchUpdateTestNullable5")));
        validateMithraResult(
                allTypesList8,
                "select * from ALL_TYPES where STRING_COL = 'BatchUpdateTest5' AND NULL_STRING_COL = 'BatchUpdateTestNullable5'",
                1000);
        assertEquals(1000, allTypesList8.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                for (AllTypes each : many)
                {
                    each.setStringValue("BatchUpdateTest5");
                    each.setNullableStringValue("BatchUpdateTestNullable6");
                }

                return null;
            }
        });

        AllTypesList allTypesList9 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("BatchUpdateTest5").and(AllTypesFinder.nullableStringValue().eq("BatchUpdateTestNullable6")));
        validateMithraResult(
                allTypesList9,
                "select * from ALL_TYPES where STRING_COL = 'BatchUpdateTest5' AND NULL_STRING_COL = 'BatchUpdateTestNullable6'",
                1000);
        assertEquals(1000, allTypesList9.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                many.setStringValue("MultiUpdateTest6");
                many.setCharValue('6');

                return null;
            }
        });

        AllTypesList allTypesList10 = AllTypesFinder.findMany(AllTypesFinder.stringValue().eq("MultiUpdateTest6").and(AllTypesFinder.charValue().eq('6')));
        validateMithraResult(
                allTypesList10,
                "select * from ALL_TYPES where STRING_COL = 'MultiUpdateTest6' AND CHAR_COL = '6'",
                1000);
        assertEquals(1000, allTypesList10.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                many.setNullableStringValue("MultiUpdateTest7");
                many.setNullableCharValue('7');

                return null;
            }
        });

        AllTypesList allTypesList11 = AllTypesFinder.findMany(AllTypesFinder.nullableStringValue().eq("MultiUpdateTest7").and(AllTypesFinder.nullableCharValue().eq('7')));
        validateMithraResult(
                allTypesList11,
                "select * from ALL_TYPES where NULL_STRING_COL = 'MultiUpdateTest7' AND NULL_CHAR_COL = '7'",
                1000);
        assertEquals(1000, allTypesList11.size());

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                many.setOrderBy(AllTypesFinder.id().ascendingOrderBy());
                for (int i = 0; i < many.size(); i++)
                {
                    many.get(i).setStringValue("Test" + i);
                }

                return null;
            }
        });

        AllTypesList allTypesList12 = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));
        allTypesList12.setOrderBy(AllTypesFinder.id().ascendingOrderBy());
        validateMithraResult(
                allTypesList12,
                "select * from ALL_TYPES where ID > " + maxId.getId(),
                1000);

        for (int i = 0; i < allTypesList12.size(); i++)
        {
            assertEquals("Test" + i, allTypesList12.get(i).getStringValue());
        }

        AllTypesFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesList many = new AllTypesList(AllTypesFinder.id().greaterThan(maxId.getId()));
                many.setOrderBy(AllTypesFinder.id().ascendingOrderBy());
                for (int i = 0; i < many.size(); i++)
                {
                    many.get(i).setStringValue("Test" + (i+1));
                    many.get(i).setIntValue(i);
                }

                return null;
            }
        });

        AllTypesList allTypesList13 = AllTypesFinder.findMany(AllTypesFinder.id().greaterThan(maxId.getId()));
        allTypesList13.setOrderBy(AllTypesFinder.id().ascendingOrderBy());
        validateMithraResult(
                allTypesList13,
                "select * from ALL_TYPES where ID > " + maxId.getId(),
                1000);

        for (int i = 0; i < allTypesList13.size(); i++)
        {
            assertEquals("Test" + (i+1), allTypesList13.get(i).getStringValue());
            assertEquals(i, allTypesList13.get(i).getIntValue());
        }
    }

    public void testDuplicatesInBatchUpdate()
    {
        final int initialId = 10000;
        final int setSize = 1000;

        final AllTypesList insertList = createNewAllTypesList(initialId, setSize);
        insertList.insertAll();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        final Operation op1 = AllTypesFinder.id().greaterThanEquals(9876);
                        AllTypesList updateList1 = new AllTypesList(op1);

                        final Operation op2 = AllTypesFinder.id().greaterThanEquals(9875);
                        AllTypesList updateList2 = new AllTypesList(op2);

                        for (int i=0; i<setSize; i++)
                        {
                            AllTypes obj1 = updateList1.get(i);
                            AllTypes obj2 = updateList2.get(i);

                            obj1.setIntValue(i + 7);
                            obj2.setIntValue(i + 7);
                        }

                        return null;
                    }
                });
    }

    public void testBatchTerminate()
    {
        int populationSize = 1000;
        TestAcctEntitySegmentList milestonedList = this.createNewMilestonedObjectList(BUSINESS_DATE, 9876, populationSize);
        milestonedList.insertAll();
        final Operation operation = TestAcctEntitySegmentFinder.businessDate().eq(BUSINESS_DATE).and(TestAcctEntitySegmentFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        final int[] retry = new int[1];

        assertFalse(TestAcctEntitySegmentFinder.getMithraObjectPortal().useMultiUpdate());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        TestAcctEntitySegmentList preUpdate = new TestAcctEntitySegmentList(operation);
                        for (TestAcctEntitySegment each : preUpdate)
                        {
                            assertEquals(InfinityTimestamp.getParaInfinity().getTime(), each.getProcessingDateTo().getTime());
                        }
                        preUpdate.terminateAll();

                        if (retry[0] == 0)
                        {
                            tx.executeBufferedOperations();
                            sleep(10000);
                        }
                        retry[0]++;
                        return null;
                    }
                }
                , new TransactionStyle(retry[0] == 0 ? 8 : 120, 5, true)
        );
        TestAcctEntitySegmentList postUpdate = TestAcctEntitySegmentFinder.findManyBypassCache(TestAcctEntitySegmentFinder.businessDate().eq(BUSINESS_DATE).and(TestAcctEntitySegmentFinder.processingDate().equalsEdgePoint()));
        assertEquals(populationSize, postUpdate.size());
        long now = System.currentTimeMillis();
        for (TestAcctEntitySegment each : postUpdate)
        {
            assertTrue(each.getProcessingDateTo().getTime() <=now);
        }
    }

    public void testSingleQueueExecutor() throws Exception
    {
        Timestamp bd1 = Timestamp.valueOf("2014-10-01 23:59:00.0");
        int count = 10000;

        final Operation operation = TestAcctEntitySegmentFinder.businessDate().eq(bd1).and(TestAcctEntitySegmentFinder.processingDate().eq(InfinityTimestamp.getParaInfinity()));

        SingleQueueExecutor executor = new SingleQueueExecutor(
                1, TestAcctEntitySegmentFinder.accountId().ascendingOrderBy(),
                200, TestAcctEntitySegmentFinder.getFinderInstance(), 5);
        executor.setInsertBatchSize(200);
        for (int i=0; i<count; i++)
        {
            TestAcctEntitySegment anObject = new TestAcctEntitySegment(bd1);
            anObject.setAccountId("" + i);
            anObject.setSegmentId(1);
            executor.addForInsert(anObject);
        }

        executor.waitUntilFinished();
        TestAcctEntitySegmentList inserted = TestAcctEntitySegmentFinder.findManyBypassCache(operation);
        assertEquals(count, inserted.size());

        Thread.sleep(1000);

        executor = new SingleQueueExecutor(
                3, TestAcctEntitySegmentFinder.accountId().ascendingOrderBy(),
                200, TestAcctEntitySegmentFinder.getFinderInstance(), 5);
        executor.setInsertBatchSize(200);

        for (TestAcctEntitySegment original : inserted)
        {
            TestAcctEntitySegment updated = new TestAcctEntitySegment(bd1, InfinityTimestamp.getParaInfinity());
            updated.setAccountId(original.getAccountId());
            updated.setSegmentId(2);

            executor.addForUpdate(original, updated);
        }

        executor.waitUntilFinished();

        List result = TestAcctEntitySegmentFinder.findManyBypassCache(operation);
        assertEquals(count, result.size());
    }

    public void testMultiUpdate()
    {
        final Operation op = AllTypesFinder.booleanValue().eq(true);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
             new TransactionalCommand()
             {
                 public Object executeTransaction(MithraTransaction tx) throws Throwable
                 {
                     AllTypesList allTypesList = new AllTypesList(op);
                     allTypesList.setBooleanValue(false);
                     return null;
                 }
             }
        );

        String sql = "select * from ALL_TYPES where BOOL_COL = "+0;
        validateMithraResult(AllTypesFinder.booleanValue().eq(false), sql);
    }

    public void testDelete()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);

        allTypes.delete();

        String sql = "select * from ALL_TYPES where ID = "+id;
        validateMithraResult(op, sql, 0);

    }

    public void testBatchDelete()
    {
        final Operation op = AllTypesFinder.booleanValue().eq(true);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
             new TransactionalCommand()
             {
                 public Object executeTransaction(MithraTransaction tx) throws Throwable
                 {
                     AllTypesList allTypesList = new AllTypesList(op);
                     for(int i = 0; i < allTypesList.size(); i++)
                     {
                         AllTypes allTypes = allTypesList.get(i);
                         allTypes.delete();
                     }

                     return null;
                 }
             }
        );

        String sql = "select * from ALL_TYPES where BOOL_COL = "+1;
        validateMithraResult(AllTypesFinder.booleanValue().eq(true), sql, 0);

    }

    public void testDeleteUsingOperation()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().greaterThanEquals(id);
        AllTypesList list = new AllTypesList(op);
        list.deleteAll();
        String sql = "select * from ALL_TYPES where ID >= "+id;
        validateMithraResult(op, sql, 0);

    }

    public void testRetrieveLimitedRows()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().greaterThanEquals(id);
        AllTypesList list = new AllTypesList(op);
        list.setMaxObjectsToRetrieve(5);
        list.setOrderBy(AllTypesFinder.id().ascendingOrderBy());
        assertEquals(5, list.size());
    }

    public void testTempTableNamer()
    {
        String first = TempTableNamer.getNextTempTableName();
        for(int i=0;i<1000000;i++)
        {
            assertFalse(first.equals(TempTableNamer.getNextTempTableName()));
        }
        UnifiedSet set = new UnifiedSet(10000);
        for(int i=0;i<10000;i++)
        {
            String s = TempTableNamer.getNextTempTableName();
            assertFalse(set.contains(s));
            set.add(s);
        }
    }

    public void testDeadLockDetectionWithFoundObjects()
    {
        for(int i=0;i<1;i++)
        {
            this.innerTestDeadlockDetectionWithFoundObjects();
        }
    }

    private OrderStatusList createNewOrderStatusList(int initialId, int size)
    {
        OrderStatusList list = new OrderStatusList();
        for(int i=0;i<size;i++)
        {
            OrderStatus s = new OrderStatus();
            s.setOrderId(initialId + i);
            s.setStatus(7);
            s.setLastUser("foo");
            s.setLastUpdateTime(new Timestamp(System.currentTimeMillis()));
            list.add(s);
        }
        return list;
    }

    public void innerTestDeadlockDetectionWithFoundObjects()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable orderCancellingThread = new Runnable(){
            public void run()
            {
                final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
                final OrderItem orderItem = order.getItems().getOrderItemAt(0);
                waitForOtherThread(rendezvous);
                final int[] tryValue = new int[1];
                MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        tx.setBulkInsertThreshold(2);
                        int initialId = 50000;
                        int setSize = 10;
                        OrderStatusList orderStatusList = createNewOrderStatusList(initialId, setSize);
                        orderStatusList.insertAll();
                        if (tryValue[0] == 0)
                        {
                            tryValue[0] = 1;
                            order.setUserId(7);
                            tx.executeBufferedOperations();
                            waitForOtherThread(rendezvous);
                            waitForOtherThread(rendezvous);
                            orderItem.setState("Cancelled");
                            order.setState("Cancelled");
                        }
                        return null;
                    }
                });
            }
        };
        Runnable orderFillingThread = new Runnable(){
            public void run()
            {
                final OrderItem item = OrderItemFinder.findOne(OrderItemFinder.orderId().eq(1));
                final Order order = item.getOrder();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                final int[] tryValue = new int[1];
                MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand() {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        tx.setBulkInsertThreshold(2);
                        int initialId = 10000;
                        int setSize = 10;
                        ProductList productList = createNewProductList(initialId, setSize);
                        productList.insertAll();
                        if (tryValue[0] == 0)
                        {
                            tryValue[0] = 1;
                            item.setState("Shipped");
                            tx.executeBufferedOperations();
                            waitForOtherThread(rendezvous);
                            order.setState("Filled");
                        }
                        return null;
                    }
                });
            }
        };
        assertTrue(runMultithreadedTest(orderCancellingThread, orderFillingThread));
    }

    public void xtestDeadConnection() throws Exception
    {
        final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        // put a break point on the next line, and kill the connection
        System.out.println("kill the connection now!");
        order.getItems().forceResolve();
    }

    public void xtestDeadConnectionInTransaction() throws Exception
    {
        final Order order = OrderFinder.findOne(OrderFinder.orderId().eq(1));
        // put a break point on the next line, and kill the connection
        System.out.println("kill the connection now!");
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand() {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.getItems().forceResolve();
                // for a second test, put a break point on the next line and kill the connection
                order.getOrderStatus();
                return null;
            }
        });
    }

    public void testSybaseTopQuery()
    {
        OrderList list = new OrderList(OrderFinder.orderId().lessThan(5));
        list.setMaxObjectsToRetrieve(1);
        assertEquals(1, list.size());
    }

    public void testSybaseTopQueryWithCursor()
    {
        OrderList list = new OrderList(OrderFinder.orderId().lessThan(5));
        list.setMaxObjectsToRetrieve(1);
        final int[] count = new int[1];
        list.forEachWithCursor(new DoWhileProcedure()
        {
            public boolean execute(Object object)
            {
                Order o = (Order) object;
                count[0]++;
                return true;
            }
        });
        assertEquals(1, count[0]);
    }

    public void testMod()
    {
        TestCalculated testCalculated = new TestCalculated();
        testCalculated.testMod();
    }

    public void testReadInNewYork()
            throws ParseException
    {
        Timestamp ts = new Timestamp(timestampFormat.parse("2008-04-01 00:00:00.0").getTime());
        Order newOrder = new Order();
        newOrder.setOrderId(999);
        newOrder.setState("Some state");
        newOrder.setTrackingId("Tracking Id");
        newOrder.setUserId(1);
        newOrder.setOrderDate(ts);

        newOrder.insert();
        OrderFinder.clearQueryCache();
        Order order = OrderFinder.findOne(OrderFinder.orderId().eq(999));
        assertEquals(ts, order.getOrderDate());
    }

    public void testDeleteAllWithIn()
    {
        IntHashSet set = new IntHashSet();
        for(int i=1;i<300;i++)
        {
            set.add(i);
        }
        Operation op = OrderFinder.orderId().in(set);
        OrderList list = new OrderList(op);
        list.deleteAll();
    }

    public void testTempObjectCreateAndDestory()
    {
        TestTempObject testTempObject = new TestTempObject();
        testTempObject.testCreateAndDestroy();
    }

    public void testTempObjectCreateAndDestoryInTransaction()
    {
        TestTempObject testTempObject = new TestTempObject();
        testTempObject.testCreateAndDestroyInTransaction();
    }

    public void testTempObjectBulkInsert()
    {
        TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();
        OrderDriverList list = new OrderDriverList();
        for(int i=0;i<1000;i++)
        {
            list.add(createOrderDriver(i));
        }
        list.bulkInsertAll();
        temporaryContext.destroy();
    }

    public void testDeleteAllInBatches()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(500);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testDeleteAllInBatchesWithTransactionTimeout()
    {
        OrderList list = createOrderList(100000, 1000);
        list.bulkInsertAll();
        MithraManagerProvider.getMithraManager().setTransactionTimeout(1);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(75000);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testDeleteAllInBatchesWithIn()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();

        IntHashSet ids = new IntHashSet(5000);
        for(int i = 1000; i < (6000);i++)
        {
            ids.add(i);
        }


        OrderList firstList = new OrderList(OrderFinder.orderId().in(ids));
        firstList.deleteAllInBatches(500);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testDeleteAllInBatchesOneByOne()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();

        list.deleteAllInBatches(500);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testDeleteAllInBatchesOneByOneWithTransactionTimeout()
    {
        OrderList list = createOrderList(50000, 1000);
        list.bulkInsertAll();

        MithraManagerProvider.getMithraManager().setTransactionTimeout(1);
        list.deleteAllInBatches(400);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }
    public void testBatchDeleteWithTransactionTimeout()
    {
        OrderList list = createOrderList(50000, 1000);
        list.bulkInsertAll();

        MithraManagerProvider.getMithraManager().setTransactionTimeout(1);
        list.deleteAllInBatches(15000);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testInsertWithDateAsString()
        throws Exception
    {
        StringDatedOrder order = new StringDatedOrder();
        int orderId = 9876;
        Timestamp ts = new Timestamp(timestampFormat.parse("2008-03-29 18:30:00.0").getTime());
        Date dt = dateFormat.parse("2008-03-29");
        order.setOrderId(orderId);
        order.setDescription("Order "+orderId);
        order.setUserId(1);
        order.setOrderDate(dt);
        order.setProcessingDate(ts);
        order.insert();
        StringDatedOrderFinder.clearQueryCache();

        StringDatedOrder order2 = StringDatedOrderFinder.findOne(StringDatedOrderFinder.orderDate().eq(dt).and(StringDatedOrderFinder.processingDate().eq(ts)));
        assertNotNull(order2);
    }

    public void testDeleteObjectWithDateAsString()
    {
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(Timestamp.valueOf("2004-01-12 00:00:00.0")));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        order.delete();
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertNull(order2);
    }

    public void testBatchDeleteWithDateAsString()
    {
        Operation op = StringDatedOrderFinder.processingDate().lessThan(new Timestamp(System.currentTimeMillis()));
        op = op.and(StringDatedOrderFinder.orderDate().lessThan(new java.util.Date()));

        StringDatedOrderList orderList = StringDatedOrderFinder.findMany(op);
        assertEquals(4, orderList.size());
        StringDatedOrderFinder.clearQueryCache();
        orderList.deleteAll();
        StringDatedOrderList orderList2 = StringDatedOrderFinder.findMany(op);
        assertEquals(0, orderList2.size());
    }

    public void testUpdateObjectWithSqlDateAsString()
                throws Exception
    {
        Timestamp ts = Timestamp.valueOf("2004-01-12 00:00:00.0");
        java.sql.Date oldDate = new java.sql.Date(ts.getTime());
        java.sql.Date newDate = new java.sql.Date(System.currentTimeMillis());
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(ts));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        assertEquals(oldDate, order.getOrderDate());
        order.setOrderDate(newDate);
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertEquals(newDate, order2.getOrderDate());
    }

    public void testUpdateObjectWithUtilDateAsString2()
        throws Exception
    {
        java.util.Date newDate = dateFormat.parse("2008-03-29");
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(Timestamp.valueOf("2004-01-12 00:00:00.0")));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        assertEquals(dateFormat.parse("2004-01-12"), order.getOrderDate());
        order.setOrderDate(newDate);
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertEquals(newDate, order2.getOrderDate());
    }

    private OrderList createOrderList(int count, int initialOrderId)
    {
        OrderList orderList = new OrderList();
        for(int i = 0; i < count; i++)
        {
            Order order = new Order();
            order.setOrderId(initialOrderId + i);
            order.setUserId(999);
            orderList.add(order);
        }
        return orderList;
    }


    private OrderDriver createOrderDriver(int orderId)
    {
        OrderDriver pd = new OrderDriver();
        pd.setOrderId(orderId);
        return pd;
    }

    public void testManyThreads() throws SQLException, InterruptedException
    {
        int readThreads = 50;
        int writeThreads = 10;

//        Connection con = SybaseTestConnectionManager.getInstance().getConnection();
//        con.createStatement().executeUpdate("ALTER TABLE ORDERS LOCK DATAROWS");
//        con.close();

        OrderList list = createOrderList(1100, 1000);
        list.insertAll();

        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);
        runManyThreads(readThreads, writeThreads, rand);
    }

    private void runManyThreads(int readThreads, int writeThreads, Random rand) throws InterruptedException
    {
        Thread[] read = new Thread[readThreads];
        for(int i=0;i<readThreads;i++)
        {
            read[i] = new ReadThread(rand);
        }

        Thread[] write = new Thread[writeThreads];
        for(int i=0;i<writeThreads;i++)
        {
            write[i] = new WriteThread(rand);
        }

        for(Thread t: read) t.start();
        for(Thread t: write) t.start();

        for(Thread t: read) t.join();
        for(Thread t: write) t.join();
    }

    private class ReadThread extends Thread
    {
        private Random rand;

        private ReadThread(Random rand)
        {
            this.rand = rand;
        }

        public void run()
        {
            for(int i=0;i<100;i++)
            {
                final int start = 1000 + rand.nextInt(500)*2;
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        IntHashSet set = new IntHashSet(10);
                        for(int j=0;j<10;j++)
                        {
                            set.add(start + j);
                        }

                        OrderList list = new OrderList(OrderFinder.orderId().in(set));
                        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                        assertEquals(10, list.size());

                        for(int i=0;i<list.size();i+=2)
                        {
                            assertEquals(list.get(i).getUserId(), list.get(i+1).getUserId());
                        }
                        return null;
                    }
                });
            }
        }
    }

    private class WriteThread extends Thread
    {
        private Random rand;

        private WriteThread(Random rand)
        {
            this.rand = rand;
        }

        public void run()
        {
            for(int i=0;i<100;i++)
            {
                final int start = 1000 + rand.nextInt(500)*2;
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        IntHashSet set = new IntHashSet(2);
                        for(int j=0;j<2;j++)
                        {
                            set.add(start + j);
                        }

                        OrderList list = new OrderList(OrderFinder.orderId().in(set));
                        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                        assertEquals(2, list.size());

                        list.get(0).setUserId(list.get(0).getUserId()+1);
                        list.get(1).setUserId(list.get(1).getUserId()+1);

                        return null;
                    }
                });
            }
        }
    }

    public void testRetrieveBitemporalMultiPkList()
            throws SQLException
    {
        TestBalanceNoAcmapList list = testForceRefresh.insertBitemporalMulitPkAndCopyToAnotherList();
        TestBalanceNoAcmap first = list.get(0);
        Operation op = TestBalanceNoAcmapFinder.businessDate().eq(first.getBusinessDate());

        TestBalanceNoAcmapList list2 = new TestBalanceNoAcmapList(op);
        assertEquals(list.size(), list2.size());
    }

    public void testForceRefreshBitemporalMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshBitemporalMultiPkList();
    }

    public void testForceRefreshBitemporalSinglePkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshBitemporalSinglePkList();
    }

    public void testForceRefreshHugeNonDatedMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshHugeNonDatedMultiPkList();
    }

    public void testForceRefreshHugeNonDatedSimpleList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshHugeNonDatedSimpleList();
    }

    public void testForceRefreshHugeSingleDatedMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshHugeSingleDatedMultiPkList();
    }

    public void testForceRefreshNonDatedMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshNonDatedMultiPkList();
    }

    public void testForceRefreshNonDatedSimpleList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshNonDatedSimpleList();
    }

    public void testForceRefreshNonDatedSimpleListInTransaction()
            throws SQLException
    {
        testForceRefresh.testForceRefreshNonDatedSimpleListInTransaction();
    }

    public void testForceRefreshOpBasedList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshOpBasedList();
    }

    public void testForceRefreshOpBasedListInTransaction()
            throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testForceRefresh.testForceRefreshOpBasedList();
                return null;
            }
        });
    }

    public void testForceRefreshSingleDatedMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshSingleDatedMultiPkList();
    }

    public void testForceRefreshHugeNonDatedMultiPkListInTransaction()
            throws SQLException
    {
        testForceRefresh.testForceRefreshHugeNonDatedMultiPkListInTransaction();
    }

    public void testForceRefreshBitemporalMultiPkListInTransaction()
            throws SQLException
    {
        testForceRefresh.testForceRefreshBitemporalMultiPkListInTransaction();
    }

    public void testDeepFetchBypassCache()
    {
        testIn.testDeepFetchBypassCache();
    }

    public void testDeepFetchWithLargeIn()
    {
        testIn.testDeepFetchWithLargeIn();
    }

    public void testDeepFetchWithLargeInWithTransaction()
    {
        testIn.testDeepFetchWithLargeInWithTransaction();
    }

    public void testLargeInWithPostDistinct()
    {
        testIn.testLargeInWithPostDistinct();
    }

    public void testLargeInWithToOneRelationshipWithOr()
    {
        testIn.testLargeInWithToOneRelationshipWithOr();
    }

    public void testTwoLargeInWithToOneRelationshipWithOr()
    {
        testIn.testTwoLargeInWithToOneRelationshipWithOr();
    }

    public void testLargeInWithToOneRelationshipWithNotExists()
    {
        testIn.xtestLargeInWithToOneRelationshipWithNotExists();
    }

    public void testLargeInWithToManyRelationshipTwoDeepWithOr()
    {
        testIn.xtestLargeInWithToManyRelationshipTwoDeepWithOr();
    }

    public void testConnectionManagerShutdown()
    {
        ((XAConnectionManager)SybaseTestConnectionManager.getInstance().getConnectionManagers().get(0)).shutdown();
    }

    public void testTupleIn()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("AA", "Product 1", 1, 100.10f);
        set.add("AA", "Product 2", 1, 120.20f);
        set.add("AB", "Product 3", 2, 300.0f);
        set.add("AB", "Product 4", 3, 400.0f);

        ProductList list = new ProductList(ProductFinder.productCode().tupleWith(ProductFinder.productDescription(),ProductFinder.manufacturerId(),ProductFinder.dailyProductionRate()).in(set));
        assertEquals(4, list.size());
    }

    public void testTupleWithBoolean()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        for(int i=9800; i <= 10000; i++)
        {
            set.add(i, true);
            set.add(i, false);
        }
        AllTypesList list = new AllTypesList(AllTypesFinder.id().tupleWith(AllTypesFinder.booleanValue()).in(set));
        assertEquals(10, list.size());
    }

    public void testBigDecimalLessThan()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG < 0.99";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThan(new BigDecimal("0.99")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalLessThanWithDouble()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG < 0.99";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThan(0.99));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalGreaterThan()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG > 0.01";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThan(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalGreaterThanWithDouble()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG > 0.01";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThan(0.01));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalLessThanEquals()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG <= 0.01";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThanEquals(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalLessThanEqualsWithDouble()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG <= 0.01";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThanEquals(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testBigDecimalGreaterThanEqualsWithBigDecimal()
            throws SQLException
    {
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());

        String sql = "select * from BIG_ORDERS where DISC_PCTG >= 0.01";
        BigOrderList orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThanEquals(0.01));

        this.genericRetrievalTest(sql, orders);
    }

    public void testGetSysLogPercentFull() throws SQLException
    {
        Connection connection = this.getConnection();
        DatabaseType databaseType = SybaseDatabaseType.getInstance();
        databaseType.getSysLogPercentFull(connection, "master");      // OK as long as it doesn't throw an exception
        connection.close();
    }

    public void testPurgeAllInBatches()
    {
        DoubleHashSet doubleOpenHashSet = new DoubleHashSet();
        doubleOpenHashSet.add(300.0);
        doubleOpenHashSet.add(400.0);
        doubleOpenHashSet.add(500.0);
        Operation findOperation = TestBalanceNoAcmapFinder.quantity().in(doubleOpenHashSet)
                .and(TestBalanceNoAcmapFinder.businessDate().equalsEdgePoint())
                .and(TestBalanceNoAcmapFinder.processingDate().equalsEdgePoint());

        Attribute[] attributes = {TestBalanceNoAcmapFinder.accountId(),
                TestBalanceNoAcmapFinder.productId(),
                TestBalanceNoAcmapFinder.positionType(),
                TestBalanceNoAcmapFinder.businessDateFrom(),
                TestBalanceNoAcmapFinder.businessDateTo(),
                TestBalanceNoAcmapFinder.processingDateFrom(),
                TestBalanceNoAcmapFinder.processingDateTo()};
        TupleAttribute tupleAttribute = new TupleAttributeImpl(attributes[0], attributes[1])
                .tupleWith(attributes[2], attributes[3], attributes[4], attributes[5], attributes[6]);

        TestBalanceNoAcmapFinder.findMany(tupleAttribute.in(TestBalanceNoAcmapFinder.findMany(findOperation), attributes)
                .and(TestBalanceNoAcmapFinder.businessDate().equalsEdgePoint())
                .and(TestBalanceNoAcmapFinder.processingDate().equalsEdgePoint())).purgeAllInBatches(500);

        assertEquals(0, TestBalanceNoAcmapFinder.findMany(findOperation).size());
    }

    public void testBatchPurgePerformance()
    {
          testBatchPurgePerformance(true, 100);
          testBatchPurgePerformance(true, 2000);

//        for (int i=100; i<10000; i+=50)
//        {
//            testBatchPurgePerformance(true, i);
//            testBatchPurgePerformance(false, i);
//        }
    }
    public void testBatchPurgePerformance(boolean useTemp, int recordCount)
    {
        final int firstId = 60000;
        TestAcctEntitySegmentList listToInsert = this.createNewMilestonedObjectList(BUSINESS_DATE, firstId, recordCount);
        listToInsert.insertAll();

        Set acctSet = new UnifiedSet();
        for (int i=firstId; i< firstId + recordCount; i++)
        {
            acctSet.add("" + i);
        }
        Operation findOperation = TestAcctEntitySegmentFinder.businessDate().eq(BUSINESS_DATE).and(TestAcctEntitySegmentFinder.processingDate().equalsEdgePoint()).and(TestAcctEntitySegmentFinder.accountId().in(acctSet));

        final TestAcctEntitySegmentList list = TestAcctEntitySegmentFinder.findManyBypassCache(findOperation);

        assertEquals(recordCount, list.size());
        long start = System.currentTimeMillis();

        if (useTemp)
        {
            this.performBatchPurge(TestAcctEntitySegmentFinder.getMithraObjectPortal(), list);
        } else
        {
            list.purgeAllInBatches(recordCount);
        }


        long end = System.currentTimeMillis();
        double v = end-start;
        v/=recordCount;
        String type = useTemp ? "batchPurge":"purgeAllInBatches";
        System.out.println(type + "," + recordCount + "," + v +"," + (end - start));
        assertEquals(0, TestAcctEntitySegmentFinder.findManyBypassCache(findOperation).size());
    }

    private void performBatchPurge(final MithraObjectPortal portal, final List list)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                portal.getMithraObjectPersister().batchPurge(list);
                return null;
            }
        });
    }


    public void testAmericasDstSwitch()
    {
        long eightPm = 1362877200000L; //2013-03-09 20:00:00.000 EST
        checkInsertRead(eightPm, 1000, true);
        checkInsertRead(eightPm + 3600000, 1001, true);
        checkInsertRead(eightPm + 2*3600000, 1002, true);
        checkInsertRead(eightPm + 3*3600000, 1003, true);
        checkInsertRead(eightPm + 4*3600000, 1004, true);
        checkInsertRead(eightPm + 5*3600000, 1005, true);
        checkInsertRead(eightPm + 6*3600000, 1006, true);
        checkInsertRead(eightPm + 7*3600000, 1007, true);
        checkInsertRead(eightPm + 8*3600000, 1008, true);
        checkInsertRead(eightPm + 9*3600000, 1009, true);
        checkInsertRead(eightPm + 10*3600000, 1010, true);
        checkInsertRead(eightPm + 11*3600000, 1011, true);
    }

    public void testAmericasEstSwitch()
    {
        long eightPm = 1351994400000L; //2012-11-03 20:00:00.000 DST
        checkInsertRead(eightPm, 1000, true);
        checkInsertRead(eightPm + 3600000, 1001, true);
        checkInsertRead(eightPm + 2*3600000, 1002, true);
        checkInsertRead(eightPm + 3*3600000, 1003, true);
        checkInsertRead(eightPm + 4*3600000, 1004, false);
        checkInsertRead(eightPm + 5*3600000, 1005, true);
        checkInsertRead(eightPm + 6*3600000, 1006, true);
        checkInsertRead(eightPm + 7*3600000, 1007, true);
        checkInsertRead(eightPm + 8*3600000, 1008, true);
        checkInsertRead(eightPm + 9*3600000, 1009, true);
        checkInsertRead(eightPm + 10*3600000, 1010, true);
        checkInsertRead(eightPm + 11*3600000, 1011, true);
    }

    private void checkInsertRead(long time, int id, boolean checkDbTime)
    {
        Timestamp nearSwitch = new Timestamp(time);
        TimezoneTest timezoneTest = new TimezoneTest();
        timezoneTest.setTimezoneTestId(id);
        timezoneTest.setDatabaseTimestamp(nearSwitch);
        timezoneTest.setUtcTimestamp(nearSwitch);
        timezoneTest.insert();
        TimezoneTest afterInsert = TimezoneTestFinder.findOneBypassCache(TimezoneTestFinder.timezoneTestId().eq(id));

        if (checkDbTime)
        {
            assertEquals(time, afterInsert.getDatabaseTimestamp().getTime());
        }
        assertEquals(time, afterInsert.getUtcTimestamp().getTime());
    }

    public void testRollbackWithTempContext() throws Exception
    {
        OrderList list = new OrderList(OrderFinder.all());
        this.executeTransactionWithTempContextThatRollbacks(list);
    }

    public void testMaxObjectsToRetrieve()
    {
        new TestBasicRetrieval().testMaxObjectsToRetrieve();
    }
}
