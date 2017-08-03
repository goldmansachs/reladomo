
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

import com.gs.collections.impl.set.mutable.primitive.IntHashSet;
import com.gs.fw.common.mithra.*;
import com.gs.fw.common.mithra.attribute.TupleAttribute;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.databasetype.SybaseIqDatabaseType;
import com.gs.fw.common.mithra.extractor.Extractor;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.aggregate.TestStandardDeviation;
import com.gs.fw.common.mithra.test.aggregate.TestVariance;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.alarm.*;
import com.gs.fw.common.mithra.transaction.TransactionStyle;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Time;
import junit.framework.Assert;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Exchanger;

public class TestSybaseIqGeneralTestCases extends MithraSybaseIqTestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private TestForceRefresh testForceRefresh = new TestForceRefresh();
    private TestIn testIn = new TestIn();

    protected void setUp() throws Exception
    {
        super.setUp();
        MithraManagerProvider.getMithraManager().setTransactionTimeout(120);
    }

    protected void tearDown() throws Exception
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(120);
        super.tearDown();
    }

    public void testTimestampGranularity2() throws Exception
    {
        new CommonVendorTestCases().testTimestampGranularity();
    }

    public void testOptimisticLocking() throws Exception
    {
        new CommonVendorTestCases().testOptimisticLocking();
    }

    public void testToCheckRepeatReadIsDisabled() throws Exception
    {
        new TestSybaseConnectionSetupForTests().testToCheckRepeatReadIsDisabled();
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
        new TestStandardDeviation(0.0000001).testStdDevBigDecimal();
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
        new TestVariance().testVariancePopBigDecimal();
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

    public void testDateYearRetrieval()
    {
        AllTypesIqList list = AllTypesIqFinder.findMany(AllTypesIqFinder.dateValue().year().eq(2007));
        assertEquals(10, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.add(2005);
        intSet.add(2007);
        Operation eq2 = AllTypesIqFinder.dateValue().year().in(intSet);
        AllTypesIqList one2 = AllTypesIqFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(10, size2);
    }

    public void testDateMonthRetrieval()
    {
        AllTypesIqList list = AllTypesIqFinder.findMany(AllTypesIqFinder.dateValue().month().eq(1));
        assertEquals(1, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesIqFinder.dateValue().month().in(intSet);
        AllTypesIqList one2 = AllTypesIqFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(4, size2);
    }

    public void testDateDayOfMonthRetrieval()
    {
        AllTypesIqList list = AllTypesIqFinder.findMany(AllTypesIqFinder.dateValue().dayOfMonth().eq(1));
        assertEquals(10, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesIqFinder.dateValue().dayOfMonth().in(intSet);
        AllTypesIqList one2 = AllTypesIqFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(10, size2);
    }

    public void testYearTimestampRetrieval() throws Exception
    {
        Operation eq = AllTypesIqFinder.timestampValue().year().eq(2007);
        AllTypesIqList many = AllTypesIqFinder.findMany(eq);
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
        AllTypesIqList list = AllTypesIqFinder.findMany(AllTypesIqFinder.timestampValue().month().eq(1));
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
        AllTypesIqList list = AllTypesIqFinder.findMany(AllTypesIqFinder.timestampValue().dayOfMonth().eq(1));
        assertEquals(1, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = AllTypesIqFinder.timestampValue().dayOfMonth().in(intSet);
        AllTypesIqList one2 = AllTypesIqFinder.findMany(eq2);
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

    public void testStringLikeEscapes()
    {
        new TestStringLike().testStringLikeEscapes();
    }

    public void testTransactionalBasicTime()
    {
        // Not using TestSybaseTimeTests().testTransactionalBasicTime() because it tests a modified scenario that depends on ASE rounding behaviour
        new TestTimeTransactional().testBasicTime();
    }

    public void testTimeTransactionalUpdate()
    {
        new TestSybaseTimeTests().testTimeTransactionalUpdate();
    }

    public void testTimeTransactionalToString()
    {
        new TestSybaseTimeTests().testTimeTransactionalToStringWithoutRounding();
    }

    public void testTimestampGranularity()
    {
        new TestSybaseTimeTests().testTimestampGranularityWithoutRounding();
    }

    public void testTimeGranularity()
    {
        new TestSybaseTimeTests().testTimeGranularityWithoutRounding();
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
        final AlarmDatedTransactionalIqList alarms = new AlarmDatedTransactionalIqList(AlarmDatedTransactionalIqFinder.all());
        alarms.addOrderBy(AlarmDatedTransactionalIqFinder.id().ascendingOrderBy());
        assertEquals("alarm 1", alarms.get(0).getDescription());
        assertEquals("alarm 2", alarms.get(1).getDescription());
        assertEquals("alarm 3", alarms.get(2).getDescription());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarms.setTime(Time.withMillis(1, 2, 3, 3));
                return null;
            }
        });

        assertEquals(3, alarms.size());
        assertEquals("01:02:03.003", alarms.get(0).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(1).getTime().toString());
        assertEquals("01:02:03.003", alarms.get(2).getTime().toString());

        Operation eq = AlarmDatedTransactionalIqFinder.processingDate().eq(new Timestamp(System.currentTimeMillis() - 100000)).and(
                AlarmDatedTransactionalIqFinder.description().eq("alarm 1")
        );
        AlarmDatedTransactionalIq alarm = AlarmDatedTransactionalIqFinder.findOne(eq);
        assertEquals(Time.withMillis(10, 30, 59, 10), alarm.getTime());
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
        Operation businessDate = AlarmBitemporalTransactionalIqFinder.businessDate().eq(Timestamp.valueOf("2012-01-01 23:59:00.0"));
        final AlarmBitemporalTransactionalIqList alarms = new AlarmBitemporalTransactionalIqList(businessDate);
        alarms.addOrderBy(AlarmBitemporalTransactionalIqFinder.id().ascendingOrderBy());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                alarms.get(0).setTimeUntil(Time.withMillis(1, 2, 3, 3), Timestamp.valueOf("2013-01-01 23:59:00.0"));
                return null;
            }
        });

        assertEquals(Time.withMillis(1, 2, 3, 3), AlarmBitemporalTransactionalIqFinder.findOne(AlarmBitemporalTransactionalIqFinder.businessDate().eq(Timestamp.valueOf("2012-12-12 23:59:00.0")).and(AlarmBitemporalTransactionalIqFinder.id().eq(1))).getTime());

        Operation op = AlarmBitemporalTransactionalIqFinder.businessDate().eq(new Timestamp(System.currentTimeMillis())).and(AlarmBitemporalTransactionalIqFinder.id().eq(1));

        assertEquals(Time.withMillis(10, 30, 59, 11), AlarmBitemporalTransactionalIqFinder.findOne(op).getTime());
    }

    public void testSimpleOrder()
    {
        Assert.assertEquals(4, new ProductList(ProductFinder.all()).size());
    }

    public void testSimpleExchangeRate()
    {
        Assert.assertEquals(5, new ExchangeRateList(ExchangeRateFinder.acmapCode().eq("A")).size());
    }

    public void testRetrieveOneRow()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().eq(id);
        String sql = "select * from ALL_TYPES_IQ where ID = "+id;
        validateMithraResult(op, sql);
    }

    public void testRetrieveOneRowUsingTimestampInOperation()
    throws ParseException
    {
        Operation op = AllTypesIqFinder.timestampValue().eq(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.990").getTime()));
        String sql = "select * from ALL_TYPES_IQ where TIMESTAMP_COL = '2007-01-01 01:01:01.990'";
        validateMithraResult(op, sql);
    }

    public void testRetrieveMultipleRows()
    throws ParseException
    {
        Operation op = AllTypesIqFinder.timestampValue().greaterThanEquals(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.990").getTime()));
        String sql = "select * from ALL_TYPES_IQ where TIMESTAMP_COL >= '2007-01-01 01:01:01.990'";
        validateMithraResult(op, sql);
    }

    public void testLargeInClause()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.bulkInsertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().in(idSet);
        String sql = "select * from ALL_TYPES_IQ where ID >= "+initialId;
        validateMithraResult(op, sql);
    }

     public void testLargeInClauseInParallel()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.bulkInsertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
        }

        AllTypesIqFinder.clearQueryCache();
        final Operation op = AllTypesIqFinder.id().in(idSet);
        final String sql = "select * from ALL_TYPES_IQ where ID >= "+initialId;


        AllTypesIqList list = new AllTypesIqList(op);
        list.setNumberOfParallelThreads(5);
        validateMithraResult(list, sql, 1);
    }

    public void testLargeInClauseBigAndSmall()
    {
        IntHashSet idSet = createIntHashSet(90);
        IntHashSet set2 = createIntHashSet(10);
        IntHashSet set3 = createIntHashSet(10);

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.intValue().in(set2).and(AllTypesIqFinder.id().in(idSet)).and(AllTypesIqFinder.nullableIntValue().in(set3));
        AllTypesIqFinder.findMany(op).forceResolve();

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

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().in(idSet).and(AllTypesIqFinder.intValue().in(idSet));
        AllTypesIqFinder.findMany(op).forceResolve();
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

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().in(idSet).and(AllTypesIqFinder.intValue().in(idSet)).and(AllTypesIqFinder.nullableIntValue().in(idSet));
        AllTypesIqFinder.findMany(op).forceResolve();
    }

    public void testLargeInClauseWithCursor()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.bulkInsertAll();

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

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().in(idSet);
        AllTypesIqList list = new AllTypesIqList(op);
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
        int setSize = 100;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        IntHashSet otherSet = new IntHashSet(setSize);
        otherSet.add(2000000000);
        for(int i = 0; i < setSize; i++)
        {
            idSet.add(initialId+i);
            otherSet.add(i);
        }

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().in(idSet).and(AllTypesIqFinder.intValue().in(otherSet));
        String sql = "select * from ALL_TYPES_IQ where ID >= "+initialId;
        validateMithraResult(op, sql);
    }

    public void testInsert()
    {
        int id = 9999999;
        AllTypesIq allTypesIqObj = this.createNewAllTypesIq(id, true);
        allTypesIqObj.insert();

        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().eq(id);
        String sql = "select * from ALL_TYPES_IQ where ID = "+id;
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

    public void testInsertCharWithSpecialChars()
    {
        int id = 9999999;
        AllTypesIq allTypesIqObj = this.createNewAllTypesIq(id, true);
        allTypesIqObj.setCharValue((char) 196);
        allTypesIqObj.setNullableCharValue((char) 196);
        allTypesIqObj.insert();

        allTypesIqObj = AllTypesIqFinder.findOneBypassCache(AllTypesIqFinder.id().eq(id));
        assertEquals(196, allTypesIqObj.getCharValue());
        assertEquals(196, allTypesIqObj.getNullableCharValue());
    }

    //todo: add tests for numeric types. Examples below have a scale of 2.
    public void testInsertFloatsAndDoublesWithRoundingIssues()
    {
        int id = 9999999;
        AllTypesIq allTypesIqObj = this.createNewAllTypesIq(id, true);
        float floatOne = Float.parseFloat("270532.60");
        double doubleOne = Double.parseDouble("270532.60");
        float floatTwo = Float.parseFloat("-98102.37");
        double doubleTwo = Double.parseDouble("-98102.37");
        allTypesIqObj.setFloatValue(floatOne);
        allTypesIqObj.setDoubleValue(doubleOne);
        allTypesIqObj.setNullableFloatValue(floatTwo);
        allTypesIqObj.setNullableDoubleValue(doubleTwo);
        allTypesIqObj.insert();

        long convertedOne = (long)((doubleOne * (Math.pow(10, 2)) + 0.5));
        long convertedTwo = Math.round(doubleTwo * (Math.pow(10, 2)));

        allTypesIqObj = AllTypesIqFinder.findOneBypassCache(AllTypesIqFinder.id().eq(id));
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

    public void testBatchInsert()
    {
        int initialId = 10000;
        int setSize = 10;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.insertAll();
        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().greaterThanEquals(initialId);
        String sql = "select * from ALL_TYPES_IQ where ID >= "+initialId;
        validateMithraResult(allTypesIqList, sql, 1);
        validateMithraResult(op, sql);
    }

    public void testBulkInsert2() throws ParseException
    {
        int initialId = 10000;
        int setSize = 200;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.get(0).setStringValue("\t tab first");
        allTypesIqList.get(1).setStringValue("tab in the middle \t of string");
        allTypesIqList.get(2).setStringValue("tab at the end \t");
        allTypesIqList.get(3).setStringValue("\n return first");
        allTypesIqList.get(4).setStringValue("return in the middle \n of string");
        allTypesIqList.get(5).setStringValue("return at the end \n");
        allTypesIqList.get(6).setStringValue("\' single quote first");
        allTypesIqList.get(7).setStringValue("single quote in the middle \' of string");
        allTypesIqList.get(8).setStringValue("single quote at the end \'");
        allTypesIqList.get(9).setStringValue("\" double quote first");
        allTypesIqList.get(10).setStringValue("double quote in the middle \" of string");
        allTypesIqList.get(11).setStringValue("double quote at the end \"");
        allTypesIqList.get(12).setStringValue("comma at the end ,");
        allTypesIqList.get(13).setStringValue(",comma at the beginning");

        AllTypesIq someNulls = allTypesIqList.get(10);
        someNulls.setNullableByteValue((byte)10);
        someNulls.setNullableCharValue('x');
        someNulls.setNullableLongValue(12345678991L);
        someNulls.setNullableDoubleValue(0.0230633307);
        someNulls.setNullableTimestampValue(new Timestamp(System.currentTimeMillis()/10*10));

        someNulls = allTypesIqList.get(11);
        someNulls.setNullableShortValue((short)-12345);
        someNulls.setNullableIntValue(856432);
        someNulls.setNullableFloatValue(0.1234576f);
        someNulls.setNullableDateValue(new java.sql.Date(dateFormat.parse("2010-01-12").getTime()));
        someNulls.setNullableStringValue(",,commas,,");

        allTypesIqList.bulkInsertAll();
        AllTypesIqFinder.clearQueryCache();
        Operation op = AllTypesIqFinder.id().greaterThanEquals(initialId);
        String sql = "select * from ALL_TYPES_IQ where ID >= "+initialId;
        validateMithraResult(allTypesIqList, sql, 1);
        validateMithraResult(op, sql);
    }

    public void testBulkInsert()
    {
        final int initialId = 10000;
        final int listSize = 200;

        bulkInsertProducts(initialId, listSize);
        Operation op = ProductFinder.productId().greaterThanEquals(initialId);
        ProductList list = new ProductList(op);
        assertEquals(listSize, list.size());
    }

    private void bulkInsertProducts(final int initialId, final int listSize)
    {
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
        AllTypesIqList allTypes = AllTypesIqFinder.findMany(AllTypesIqFinder.all());
        allTypes.setOrderBy(AllTypesIqFinder.id().descendingOrderBy());

        assertTrue(allTypes.size() > 0);

        final AllTypesIq maxId = allTypes.get(0).getNonPersistentCopy();

        AllTypesIqList newAllTypesList = new AllTypesIqList();
        for (int i = 1; i <= 1000; i++)
        {
            AllTypesIq newAllTypes = new AllTypesIq();
            newAllTypes.setId(maxId.getId() + i);
            newAllTypes.copyNonPrimaryKeyAttributesFrom(maxId);

            newAllTypesList.add(newAllTypes);
        }

        newAllTypesList.insertAll();

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest");

                return null;
            }
        });

        AllTypesIqList allTypesIqList1 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("MultiUpdateTest"));

        validateMithraResult(
                allTypesIqList1,
                "select * from ALL_TYPES_IQ where STRING_COL = 'MultiUpdateTest'",
                1000);
        assertEquals(1000, allTypesIqList1.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));

                many.setNullableStringValue("MultiUpdateTestNullable");

                return null;
            }
        });

        AllTypesIqList allTypesIqList2 = AllTypesIqFinder.findMany(AllTypesIqFinder.nullableStringValue().eq("MultiUpdateTestNullable"));
        validateMithraResult(
                allTypesIqList2,
                "select * from ALL_TYPES_IQ where NULL_STRING_COL = 'MultiUpdateTestNullable'",
                1000);
        assertEquals(1000, allTypesIqList2.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest2");
                many.setNullableStringValue("MultiUpdateTestNullable2");

                return null;
            }
        });

        AllTypesIqList allTypesIqList3 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("MultiUpdateTest2").and(AllTypesIqFinder.nullableStringValue().eq("MultiUpdateTestNullable2")));
        validateMithraResult(
                allTypesIqList3,
                "select * from ALL_TYPES_IQ where STRING_COL = 'MultiUpdateTest2' AND NULL_STRING_COL = 'MultiUpdateTestNullable2'",
                1000);
        assertEquals(1000, allTypesIqList3.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest2");
                many.setNullableStringValue("MultiUpdateTest2");

                return null;
            }
        });

        AllTypesIqList allTypesIqList4 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("MultiUpdateTest2").and(AllTypesIqFinder.nullableStringValue().eq("MultiUpdateTest2")));
        validateMithraResult(
                allTypesIqList4,
                "select * from ALL_TYPES_IQ where STRING_COL = 'MultiUpdateTest2' AND NULL_STRING_COL = 'MultiUpdateTest2'",
                1000);
        assertEquals(1000, allTypesIqList4.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));

                many.setStringValue("MultiUpdateTest3");
                many.setNullableStringValue("MultiUpdateNullableTest3");
                many.setStringValue("MultiUpdateTest4");
                many.setNullableStringValue("MultiUpdateNullableTest4");

                return null;
            }
        });

        AllTypesIqList allTypesIqList5 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("MultiUpdateTest4").and(AllTypesIqFinder.nullableStringValue().eq("MultiUpdateNullableTest4")));
        validateMithraResult(
                allTypesIqList5,
                "select * from ALL_TYPES_IQ where STRING_COL = 'MultiUpdateTest4' AND NULL_STRING_COL = 'MultiUpdateNullableTest4'",
                1000);
        assertEquals(1000, allTypesIqList5.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest");
                    each.setNullableStringValue("BatchUpdateTestNullable");
                }

                return null;
            }
        });

        AllTypesIqList allTypesIqList6 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("BatchUpdateTest").and(AllTypesIqFinder.nullableStringValue().eq("BatchUpdateTestNullable")));
        validateMithraResult(
                allTypesIqList6,
                "select * from ALL_TYPES_IQ where STRING_COL = 'BatchUpdateTest' AND NULL_STRING_COL = 'BatchUpdateTestNullable'",
                1000);
        assertEquals(1000, allTypesIqList6.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest2");
                    each.setNullableStringValue("BatchUpdateTestNullable2");
                }

                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest3");
                }

                for (AllTypesIq each : many)
                {
                    each.setNullableStringValue("BatchUpdateTestNullable3");
                }

                return null;
            }
        });

        AllTypesIqList allTypesIqList7 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("BatchUpdateTest3").and(AllTypesIqFinder.nullableStringValue().eq("BatchUpdateTestNullable3")));
        validateMithraResult(
                allTypesIqList7,
                "select * from ALL_TYPES_IQ where STRING_COL = 'BatchUpdateTest3' AND NULL_STRING_COL = 'BatchUpdateTestNullable3'",
                1000);
        assertEquals(1000, allTypesIqList7.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest4");
                    each.setNullableStringValue("BatchUpdateTestNullable4");
                }

                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest5");
                    each.setNullableStringValue("BatchUpdateTestNullable5");
                }

                return null;
            }
        });

        AllTypesIqList allTypesIqList8 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("BatchUpdateTest5").and(AllTypesIqFinder.nullableStringValue().eq("BatchUpdateTestNullable5")));
        validateMithraResult(
                allTypesIqList8,
                "select * from ALL_TYPES_IQ where STRING_COL = 'BatchUpdateTest5' AND NULL_STRING_COL = 'BatchUpdateTestNullable5'",
                1000);
        assertEquals(1000, allTypesIqList8.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                for (AllTypesIq each : many)
                {
                    each.setStringValue("BatchUpdateTest5");
                    each.setNullableStringValue("BatchUpdateTestNullable6");
                }

                return null;
            }
        });

        AllTypesIqList allTypesIqList9 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("BatchUpdateTest5").and(AllTypesIqFinder.nullableStringValue().eq("BatchUpdateTestNullable6")));
        validateMithraResult(
                allTypesIqList9,
                "select * from ALL_TYPES_IQ where STRING_COL = 'BatchUpdateTest5' AND NULL_STRING_COL = 'BatchUpdateTestNullable6'",
                1000);
        assertEquals(1000, allTypesIqList9.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                many.setStringValue("MultiUpdateTest6");
                many.setCharValue('6');

                return null;
            }
        });

        AllTypesIqList allTypesList10 = AllTypesIqFinder.findMany(AllTypesIqFinder.stringValue().eq("MultiUpdateTest6").and(AllTypesIqFinder.charValue().eq('6')));
        validateMithraResult(
                allTypesList10,
                "select * from ALL_TYPES_IQ where STRING_COL = 'MultiUpdateTest6' AND CHAR_COL = '6'",
                1000);
        assertEquals(1000, allTypesList10.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                many.setNullableStringValue("MultiUpdateTest7");
                many.setNullableCharValue('7');

                return null;
            }
        });

        AllTypesIqList allTypesList11 = AllTypesIqFinder.findMany(AllTypesIqFinder.nullableStringValue().eq("MultiUpdateTest7").and(AllTypesIqFinder.nullableCharValue().eq('7')));
        validateMithraResult(
                allTypesList11,
                "select * from ALL_TYPES_IQ where NULL_STRING_COL = 'MultiUpdateTest7' AND NULL_CHAR_COL = '7'",
                1000);
        assertEquals(1000, allTypesList11.size());

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                many.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
                for (int i = 0; i < many.size(); i++)
                {
                    many.get(i).setStringValue("Test" + i);
                }

                return null;
            }
        });

        AllTypesIqList allTypesList12 = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));
        allTypesList12.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
        validateMithraResult(
                allTypesList12,
                "select * from ALL_TYPES_IQ where ID > " + maxId.getId(),
                1000);

        for (int i = 0; i < allTypesList12.size(); i++)
        {
            assertEquals("Test" + i, allTypesList12.get(i).getStringValue());
        }

        AllTypesIqFinder.clearQueryCache();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AllTypesIqList many = new AllTypesIqList(AllTypesIqFinder.id().greaterThan(maxId.getId()));
                many.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
                for (int i = 0; i < many.size(); i++)
                {
                    many.get(i).setStringValue("Test" + (i+1));
                    many.get(i).setIntValue(i);
                }

                return null;
            }
        });

        AllTypesIqList allTypesList13 = AllTypesIqFinder.findMany(AllTypesIqFinder.id().greaterThan(maxId.getId()));
        allTypesList13.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
        validateMithraResult(
                allTypesList13,
                "select * from ALL_TYPES_IQ where ID > " + maxId.getId(),
                1000);

        for (int i = 0; i < allTypesList13.size(); i++)
        {
            assertEquals("Test" + (i+1), allTypesList13.get(i).getStringValue());
            assertEquals(i, allTypesList13.get(i).getIntValue());
        }
    }

    public void testTupleInWithSharedTempTables()
    {
        runTupleIn(SybaseIqDatabaseType.getInstance());
    }

    public void testTupleInWithUnSharedTempTables()
    {
        try
        {
            runTupleIn(SybaseIqDatabaseType.getInstanceWithoutSharedTempTables());
        }
        finally
        {
            SybaseIqTestConnectionManager.getInstance().setDatabaseType(SybaseIqDatabaseType.getInstance());
        }

    }

    public void runTupleIn(SybaseIqDatabaseType databaseType)
    {
        SybaseIqTestConnectionManager.getInstance().setDatabaseType(databaseType);
        final int initialId = 10000;
        final int listSize = 200;
        bulkInsertProducts(initialId, listSize);
        ProductList productList = createNewProductList(initialId, listSize);
        TupleAttribute tupleAttribute = ProductFinder.productId().tupleWith(ProductFinder.productCode()).tupleWith(ProductFinder.productDescription());
        Operation op = tupleAttribute.in(productList, new Extractor[]{ProductFinder.productId(), ProductFinder.productCode(), ProductFinder.productDescription()});
        assertEquals(listSize, ProductFinder.findManyBypassCache(op).size());

    }

    public void testSingleUpdate()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().eq(id);
        AllTypesIq allTypesIq = AllTypesIqFinder.findOne(op);
        allTypesIq.setIntValue(100);
        String sql = "select * from ALL_TYPES_IQ where ID = "+9876;
        validateMithraResult(op, sql);
    }

    public void testMultipleUpdatesToSameObjectWithNoTransaction()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().eq(id);
        AllTypesIq allTypesIq = AllTypesIqFinder.findOne(op);
        allTypesIq.setIntValue(100);
        allTypesIq.setNullableByteValue((byte)10);
        allTypesIq.setNullableCharValue('a');
        allTypesIq.setNullableShortValue((short)1000);
        allTypesIq.setNullableIntValue(987654);
        String sql = "select * from ALL_TYPES_IQ where ID = "+9876;
        validateMithraResult(op, sql);
    }

    public void testMultipleUpdatesToSameObjectInTransaction()
    {
        int id = 9876;
        final Operation op = AllTypesIqFinder.id().eq(id);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
             new TransactionalCommand()
             {
                 public Object executeTransaction(MithraTransaction tx) throws Throwable
                 {
                    AllTypesIq allTypesIq = AllTypesIqFinder.findOne(op);
                    allTypesIq.setIntValue(100);
                    allTypesIq.setNullableByteValue((byte)10);
                    allTypesIq.setNullableCharValue('a');
                    allTypesIq.setNullableShortValue((short)1000);
                    allTypesIq.setNullableIntValue(987654);
                    return null;
                 }
             }
        );
        String sql = "select * from ALL_TYPES_IQ where ID = "+9876;
        validateMithraResult(op, sql);
    }


    public void testBatchUpdate()
    {
        int initialId = 10000;
        int setSize = 1000;
        int maxLoop = 4;
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.bulkInsertAll();
        final Operation op = AllTypesIqFinder.id().greaterThanEquals(9876);

        final int[] retry = new int[1];
        int caughtExceptions = 0;
        for(int k=0;k<maxLoop;k++)
        {
            try
            {
                final int count = k;
                long start = System.currentTimeMillis();
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                     new TransactionalCommand()
                     {
                         public Object executeTransaction(MithraTransaction tx) throws Throwable
                         {
//                             System.out.println("retry[0] "+retry[0]+" tx timeout "+tx.getTimeoutInMilliseconds());
                             AllTypesIqList allTypesIqList = new AllTypesIqList(op);
                             allTypesIqList.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
                             for(int i = 0; i < allTypesIqList.size()/2; i++)
                             {
                                 if (retry[0] <= 1)
                                 {
                                     assertEquals(2000000000, allTypesIqList.get(i).getIntValue());
                                 }
                                 AllTypesIq allTypesIq  = allTypesIqList.get(i);
                                 allTypesIq.setIntValue(i+count);
                             }
                             retry[0]++;
                             if (retry[0] == 1)
                             {
                                 tx.executeBufferedOperations();
                                 sleep(10000);
                             }
                             for(int i = allTypesIqList.size()/2; i < allTypesIqList.size(); i++)
                             {
                                 if (retry[0] <= 1)
                                 {
                                     assertEquals(2000000000, allTypesIqList.get(i).getIntValue());
                                 }
                                 AllTypesIq allTypesIq  = allTypesIqList.get(i);
                                 allTypesIq.setIntValue(i+count);
                             }
                             return null;
                         }
                     }
                , new TransactionStyle(retry[0] == 0 ? 8 : 120, retry[0] == 0 ? 2 : 5, true));
//                System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
            }
            catch(MithraTransactionException e)
            {
                caughtExceptions++;
                if (caughtExceptions > 1)
                {
                    throw e;
                }
            }
        }

        String sql = "select * from ALL_TYPES_IQ where ID >= "+9876;
        validateMithraResult(op, sql);
        allTypesIqList = new AllTypesIqList(op);
        allTypesIqList.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
        for(int i = 0; i < allTypesIqList.size(); i++)
        {
            AllTypesIq allTypesIq  = allTypesIqList.get(i);
            assertEquals(i+maxLoop-1, allTypesIq.getIntValue());
        }
    }

    public void testMultiUpdate()
    {
        int initialId = 10000;
        int setSize = 1000;
        int maxLoop = 3; // must be odd!
        AllTypesIqList allTypesIqList = this.createNewAllTypesIqList(initialId, setSize);
        allTypesIqList.bulkInsertAll();
        final Operation opTrue = AllTypesIqFinder.booleanValue().eq(true);
        final Operation opFalse = AllTypesIqFinder.booleanValue().eq(false);

        for(int k=0;k<maxLoop;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         AllTypesIqList allTypesIqList = new AllTypesIqList((count % 2) != 0 ? opFalse : opTrue);
                         allTypesIqList.setBooleanValue((count % 2) != 0);
                         return null;
                     }
                 }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        String sql = "select * from ALL_TYPES_IQ where BOOL_COL = "+0;
        validateMithraResult(AllTypesIqFinder.booleanValue().eq(false), sql);
    }

    public void testBatchUpdateNullablePk() throws ParseException
    {
        int initialId = 10000;
        int setSize = 100;
        int maxLoop = 4;
        ExchangeRateList exchangeRateList = this.createNewExchangeRateList(setSize);
        exchangeRateList.bulkInsertAll();
        final Operation op = ExchangeRateFinder.source().isNull().and(ExchangeRateFinder.acmapCode().eq("A"));

        for(int k=0;k<maxLoop;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         ExchangeRateList exchangeRateList = new ExchangeRateList(op);
                         exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
                         for(int i = 0; i < exchangeRateList.size(); i++)
                         {
                             ExchangeRate exchangeRate  = exchangeRateList.get(i);
                             exchangeRate.setExchangeRate(i+count);
                         }
                         return null;
                     }
                 }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        exchangeRateList = new ExchangeRateList(op);
        exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
        exchangeRateList.setBypassCache(true);
        for(int i = 0; i < exchangeRateList.size(); i++)
        {
            ExchangeRate exchangeRate  = exchangeRateList.get(i);
            assertEquals(i+maxLoop-1, exchangeRate.getExchangeRate(), 0.0);
        }
    }

    public void testBatchUpdateMutablePk() throws ParseException
    {
        int setSize = 100;
        int maxLoop = 4;
        ExchangeRateList exchangeRateList = this.createNewExchangeRateList(setSize);
        exchangeRateList.bulkInsertAll();
        final Operation op = ExchangeRateFinder.acmapCode().eq("A");

        for(int k=0;k<maxLoop;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         ExchangeRateList exchangeRateList = new ExchangeRateList(op);
                         exchangeRateList.deepFetch(ExchangeRateFinder.children());
                         exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
                         for(int i = 0; i < exchangeRateList.size(); i++)
                         {
                             ExchangeRate exchangeRate  = exchangeRateList.get(i);
                             if (count > 0)
                             {
                                 assertEquals(i+count - 1, exchangeRate.getSource());
                             }
                             exchangeRate.setSource(i+count);
                         }
                         return null;
                     }
                 }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        exchangeRateList = new ExchangeRateList(op);
        exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
        exchangeRateList.setBypassCache(true);
        assertTrue(exchangeRateList.size() > 0);
        for(int i = 0; i < exchangeRateList.size(); i++)
        {
            ExchangeRate exchangeRate  = exchangeRateList.get(i);
            assertEquals(i+maxLoop-1, exchangeRate.getSource());
        }
    }

    private ExchangeRateList createNewExchangeRateList(int setSize) throws ParseException
    {
        ExchangeRateList result = new ExchangeRateList();
        Timestamp now = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        for(int i=0;i<setSize;i++)
        {
            ExchangeRate b = new ExchangeRate();
            b.setAcmapCode("A");
            b.setSourceNull();
            b.setCurrency(""+i);
            b.setDate(now);
            result.add(b);
        }
        return result;
    }


    public void testMultiUpdateDated() throws ParseException
    {
        int initialId = 10000;
        int setSize = 1000;
        int maxLoop = 3; // must be odd!
        TinyBalanceList tinyBalanceList = this.createNewTinyBalanceList(initialId, setSize);
        tinyBalanceList.bulkInsertAll();
        final Operation op = TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThanEquals(initialId)).
                and(TinyBalanceFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2008-10-10 00:00:00.000").getTime())));

        for(int k=0;k<maxLoop;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         tx.setBulkInsertThreshold(10);
                         TinyBalanceList tinyBalanceList = new TinyBalanceList(op);
                         tinyBalanceList.setQuantity(count);
                         return null;
                     }
                 }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }
        tinyBalanceList = new TinyBalanceList(op);
        tinyBalanceList.setBypassCache(true);
        assertTrue(tinyBalanceList.size() > 0);
        for(TinyBalance b: tinyBalanceList)
        {
            assertEquals(maxLoop - 1, b.getQuantity(), 0.0);
        }
    }

    public void testMultiUpdateDatedOptimisticFailure() throws ParseException, SQLException
    {
        int initialId = 10000;
        int setSize = 100;
        TinyBalanceList tinyBalanceList = this.createNewTinyBalanceList(initialId, setSize);
        tinyBalanceList.bulkInsertAll();
        final Operation op = TinyBalanceFinder.acmapCode().eq("A").and(TinyBalanceFinder.balanceId().greaterThanEquals(initialId)).
                and(TinyBalanceFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2008-10-10 00:00:00.000").getTime())));
        final TinyBalanceList toUpdateList = new TinyBalanceList(op);
        toUpdateList.forceResolve();

        Connection con = SybaseIqTestConnectionManager.getInstance().getConnection();
        con.createStatement().execute("update TINY_BALANCE set IN_Z = '"+timestampFormat.format(new Timestamp(System.currentTimeMillis()+10))+"' where BALANCE_ID in (10005, 10010)");
        con.close();

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         tx.setBulkInsertThreshold(10);
                         TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                         toUpdateList.setQuantity(100);
                         return null;
                     }
                 }
            );
        }
        catch (MithraOptimisticLockException e)
        {
            // expected
        }
    }

    private TinyBalanceList createNewTinyBalanceList(int initialId, int setSize) throws ParseException
    {
        TinyBalanceList result = new TinyBalanceList();
        Timestamp now = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        for(int i=0;i<setSize;i++)
        {
            TinyBalance b = new TinyBalance(now);
            b.setAcmapCode("A");
            b.setBalanceId(i+initialId);
            b.setQuantity(-1);
            result.add(b);
        }
        return result;
    }

    public void testDelete()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().eq(id);
        AllTypesIq allTypesIq = AllTypesIqFinder.findOne(op);

        allTypesIq.delete();

        String sql = "select * from ALL_TYPES_IQ where ID = "+id;
        validateMithraResult(op, sql, 0);

    }

    public void testBatchDelete()
    {
        final Operation op = AllTypesIqFinder.booleanValue().eq(true);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
             new TransactionalCommand()
             {
                 public Object executeTransaction(MithraTransaction tx) throws Throwable
                 {
                     AllTypesIqList allTypesIqList = new AllTypesIqList(op);
                     for(int i = 0; i < allTypesIqList.size(); i++)
                     {
                         AllTypesIq allTypesIq = allTypesIqList.get(i);
                         allTypesIq.delete();
                     }

                     return null;
                 }
             }
        );

        String sql = "select * from ALL_TYPES_IQ where BOOL_COL = "+1;
        validateMithraResult(AllTypesIqFinder.booleanValue().eq(true), sql, 0);

    }

    public void testDeleteUsingOperation()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().greaterThanEquals(id);
        AllTypesIqList list = new AllTypesIqList(op);
        list.deleteAll();
        String sql = "select * from ALL_TYPES_IQ where ID >= "+id;
        validateMithraResult(op, sql, 0);

    }

    public void testRetrieveLimitedRows()
    {
        int id = 9876;
        Operation op = AllTypesIqFinder.id().greaterThanEquals(id);
        AllTypesIqList list = new AllTypesIqList(op);
        list.setMaxObjectsToRetrieve(5);
        list.setOrderBy(AllTypesIqFinder.id().ascendingOrderBy());
        assertEquals(5, list.size());
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
        IntHashSet set = new IntHashSet(300);
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
        assertEquals(1000, OrderDriverFinder.findManyBypassCache(OrderDriverFinder.all()).size());
        temporaryContext.destroy();
    }

    public void testTempObjectBulkInsertInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testTempObjectBulkInsert();
                return null;
            }
        });
    }

    public void testTempObjectBulkInsertInTransactionUnshared()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testTempObjectBulkInsertWithUnSharedTempTables();
                return null;
            }
        });
    }

    public void testTempObjectBulkInsertWithUnSharedTempTables()
    {
        try
        {
            SybaseIqTestConnectionManager.getInstance().setDatabaseType(SybaseIqDatabaseType.getInstanceWithoutSharedTempTables());
            testTempObjectBulkInsert();
        }
        finally
        {
            SybaseIqTestConnectionManager.getInstance().setDatabaseType(SybaseIqDatabaseType.getInstance());
        }

    }

    /* todo: */ public void fixmetestDeleteAllInBatches()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(500);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    /* todo: */ public void fixmetestDeleteAllInBatchesWithTransactionTimeout()
    {
        OrderList list = createOrderList(100000, 1000);
        list.bulkInsertAll();
        MithraManagerProvider.getMithraManager().setTransactionTimeout(1);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(75000);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    /* todo: */ public void fixmetestDeleteAllInBatchesWithIn()
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

    /* todo: */ public void fixmetestDeleteAllInBatchesOneByOne()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();

        list.deleteAllInBatches(500);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    /* todo: */ public void fixmetestDeleteAllInBatchesOneByOneWithTransactionTimeout()
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
        op = op.and(StringDatedOrderFinder.orderDate().lessThan(new Date()));

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
        Date newDate = dateFormat.parse("2008-03-29");
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

    public void testForceRefreshBitemporalMultiPkList()
            throws SQLException
    {
        testForceRefresh.testForceRefreshBitemporalMultiPkList();
    }

    public void testRetrieveBitemporalMultiPkList()
            throws SQLException
    {
        TestBalanceNoAcmapList list = testForceRefresh.insertBitemporalMulitPkAndCopyToAnotherList();
        TestBalanceNoAcmap first = list.get(0);
//        Operation op = TestBalanceNoAcmapFinder.businessDate().eq(first.getBusinessDate());
        Operation op = TestBalanceNoAcmapFinder.businessDate().equalsEdgePoint();
        //note: the following operation is necessary for IQ because it can't handle OUT_Z = '9999-12-01 23:59' correctly
        op = op.and(TestBalanceNoAcmapFinder.processingDate().equalsEdgePoint());

        TestBalanceNoAcmapList list2 = new TestBalanceNoAcmapList(op);
        assertEquals(list.size(), list2.size());
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

    public void testUniqueIndexViolationForSybaseInsert()
    {
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Product product = new Product();
                    product.setProductId(1);
                    product.insert();
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForSybaseMultiInsert()
    {
        try
        {
            final ProductList productList = new ProductList();

            // default batch size is 32, so create product objects with 2 batches which should all get inserted successfully
            for(int i = 5; i < 69; i++ )
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            // now add elements which violate unique index
            for(int i = 1; i < 33; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            productList.insertAll();
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 31"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForSybaseBulkInsert()
    {
        final int listSize = 150;
        try
        {
            final ProductList productList = new ProductList();

            for (int i=0; i<listSize; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }
            productList.bulkInsertAll();
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 31"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForSybaseUpdate() throws SQLException
    {
        Connection con = SybaseIqTestConnectionManager.getInstance().getConnection();
        String dropSql = "if exists (select name from mithra_qa.dbo.sysindexes where name = 'PROD_DESC' and id=object_id('PRODUCT')) " +
                                                    "drop index PRODUCT.PROD_DESC";
        con.createStatement().execute(dropSql);
        StringBuffer uniqueIndexSql = new StringBuffer("create unique index PROD_DESC on PRODUCT(PROD_DESC)");
        con.createStatement().execute(uniqueIndexSql.toString());

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    Product product = ProductFinder.findOne(ProductFinder.productId().eq(2));
                    product.setProductDescription("Product 1");
                    return null;
                }
            });
        }
        catch(MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 2"));
        }
        catch(RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
        finally
        {
            if (con == null)
            {
                con = SybaseTestConnectionManager.getInstance().getConnection();
            }
            con.createStatement().execute(dropSql);
            con.rollback();
            con.close();
        }
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

    private IntHashSet createOrdersAndItems()
    {
        OrderList orderList = new OrderList();
        for (int i = 0; i < 1100; i++)
        {
            Order order = new Order();
            order.setOrderId(i+1000);
            order.setDescription("order number "+i);
            order.setUserId(i+7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setTrackingId("T"+i+1000);
            orderList.add(order);
        }
        orderList.bulkInsertAll();
        OrderItemList items = new OrderItemList();
        IntHashSet itemIds = new IntHashSet();
        for (int i = 0; i < 1100; i++)
        {
            OrderItem item = new OrderItem();
            item.setOrderId(i+1000);
            item.setId(i+1000);
            items.add(item);

            item = new OrderItem();
            item.setOrderId(i+1000);
            item.setId(i+3000);
            item.setProductId((i % 4) + 1);
            items.add(item);

            itemIds.add(i+1000);
            itemIds.add(i+3000);
        }
        items.bulkInsertAll();
        return itemIds;
    }

    public void testOneToManyOneToOneSameAttribute()
    {
        createOrdersAndItems();
        OrderList orderList = new OrderList(OrderFinder.findMany(OrderFinder.orderId().greaterThan(5)));
        orderList.deepFetch(OrderFinder.items());
        orderList.deepFetch(OrderFinder.items().productInfo());
        orderList.deepFetch(OrderFinder.orderStatus());

        assertTrue(orderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<orderList.size();i++)
        {
            OrderItemList items = orderList.get(i).getItems();
            itemCount += items.size();

            for(OrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            orderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testOneToManyOneToOneSameAttributeInParallel()
    {
        createOrdersAndItems();
        OrderList orderList = new OrderList(OrderFinder.orderId().greaterThan(5));
        orderList.deepFetch(OrderFinder.items());
        orderList.deepFetch(OrderFinder.items().productInfo());
        orderList.deepFetch(OrderFinder.orderStatus());
        orderList.setNumberOfParallelThreads(3);

        assertTrue(orderList.size() > 0);
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        int itemCount = 0;
        int prodCount = 0;
        for(int i=0;i<orderList.size();i++)
        {
            OrderItemList items = orderList.get(i).getItems();
            itemCount += items.size();

            for(OrderItem item: items)
            {
                if (item.getProductInfo() != null) prodCount++;
            }
            orderList.get(i).getOrderStatus();
        }
        assertTrue(itemCount > 0);
        assertTrue(prodCount > 0);
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testAmericasDstSwitch()
    {
        long eightPm = 1362877200000L; //2013-03-09 20:00:00.000 EST
        checkInsertRead(eightPm, 1000, true);
        checkInsertRead(eightPm + 3600000, 1001, true);
        checkInsertRead(eightPm + 2 * 3600000, 1002, true);
        checkInsertRead(eightPm + 3 * 3600000, 1003, true);
        checkInsertRead(eightPm + 4 * 3600000, 1004, true);
        checkInsertRead(eightPm + 5 * 3600000, 1005, true);
        checkInsertRead(eightPm + 6*3600000, 1006, true);
        checkInsertRead(eightPm + 7*3600000, 1007, true);
        checkInsertRead(eightPm + 8*3600000, 1008, true);
        checkInsertRead(eightPm + 9 * 3600000, 1009, true);
        checkInsertRead(eightPm + 10 * 3600000, 1010, true);
        checkInsertRead(eightPm + 11 * 3600000, 1011, true);
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

    public void testBulkLoaderOnBooleanOnNumericColumnsWithNullablesNull() throws Exception
    {
        this.bulkInsertAndAssertBooleanOnNumericColumns(true);
    }

    public void testBulkLoaderOnBooleanOnNumericColumnsWithNullablesPopulated() throws Exception
    {
        this.bulkInsertAndAssertBooleanOnNumericColumns(false);
    }

    private void bulkInsertAndAssertBooleanOnNumericColumns(boolean withNullablesNull)
    {
        this.setMithraTestObjectToResultSetComparator(new BooleansOnNumericColumnsIqResultSetComparator());

        final BooleansOnNumericColumnsIqList booleansOnNumericColumnsIqList = this.createNewBooleansOnNumericColumnsIqList(0, 500, withNullablesNull);
        booleansOnNumericColumnsIqList.bulkInsertAll();

        assertEquals(1, BooleansOnNumericColumnsIqFinder.getMithraObjectPortal().getPerformanceData().getDataForInsert().getTotalOperations());

        String sql = "select * from BOOLEAN_ON_NUMERIC_TYPES";
        validateMithraResult(booleansOnNumericColumnsIqList, sql, 1);
        validateMithraResult(BooleansOnNumericColumnsIqFinder.findMany(BooleansOnNumericColumnsIqFinder.all()), sql, 1);
    }

    private BooleansOnNumericColumnsIqList createNewBooleansOnNumericColumnsIqList(int firstId, long count, boolean withNullablesNull)
    {
        BooleansOnNumericColumnsIqList list = new BooleansOnNumericColumnsIqList();
        for(int i = firstId; i < (firstId + count); i++)
        {
            final boolean isPairId = i % 2 == 0;

            BooleansOnNumericColumnsIq booleanObj = new BooleansOnNumericColumnsIq();

            booleanObj.setId(i);
            booleanObj.setBooleanValue(isPairId);
            booleanObj.setBooleanOnByteValue(!isPairId);
            booleanObj.setBooleanOnShortValue(isPairId);
            booleanObj.setBooleanOnIntValue(!isPairId);
            booleanObj.setBooleanOnLongValue(isPairId);

            if(withNullablesNull)
            {
                booleanObj.setNullablePrimitiveAttributesToNull();
            }
            else
            {
                booleanObj.setNullableBooleanValue(booleanObj.isBooleanValue());
                booleanObj.setNullableBooleanOnByteValue(booleanObj.isBooleanOnByteValue());
                booleanObj.setNullableBooleanOnShortValue(booleanObj.isBooleanOnShortValue());
                booleanObj.setNullableBooleanOnIntValue(booleanObj.isBooleanOnIntValue());
                booleanObj.setNullableBooleanOnLongValue(booleanObj.isBooleanOnLongValue());
            }

            list.add(booleanObj);
        }
        return list;
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
