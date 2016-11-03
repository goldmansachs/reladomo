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
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.integer.IntegerResultSetParser;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanList;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanListForSubclass;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanListImmutability;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanListOrderBy;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanListWithHavingClause;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateBeanListWithPrimitives;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateListWithOrderBy;
import com.gs.fw.common.mithra.test.aggregate.TestAggregatePrimitiveBeanListWithHavingClause;
import com.gs.fw.common.mithra.test.aggregate.TestAggregateWithNull;
import com.gs.fw.common.mithra.test.aggregate.TestAggregationWithHavingClause;
import com.gs.fw.common.mithra.test.aggregate.TestAvg;
import com.gs.fw.common.mithra.test.aggregate.TestCount;
import com.gs.fw.common.mithra.test.aggregate.TestMax;
import com.gs.fw.common.mithra.test.aggregate.TestMin;
import com.gs.fw.common.mithra.test.aggregate.TestNumericAttribute;
import com.gs.fw.common.mithra.test.aggregate.TestSum;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.dated.AuditedOrderStatusTwo;
import com.gs.fw.common.mithra.test.inherited.TestReadOnlyInherited;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;



public class TestClientPortal extends RemoteMithraServerTestCase
{

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    protected Class[] getRestrictedClassList()
    {
        HashSet<Class> result = new HashSet<Class>();
        addTestClassesFromOther(new TestOrderby(), result);
        addTestClassesFromOther(new TestAdditionalRelationships(""), result);
        addTestClassesFromOther(new SelfJoinTest(), result);
        addTestClassesFromOther(new TestDatedBasicRetrieval(), result);
        addTestClassesFromOther(new TestBasicBooleanOperation(), result);
        addTestClassesFromOther(new TestBasicRetrieval(), result);
        addTestClassesFromOther(new TestBasicTransactionalRetrieval(), result);
        addTestClassesFromOther(new TestDatedWithNotDatedJoin(), result);
        addTestClassesFromOther(new TestMappedOperation(), result);
        addTestClassesFromOther(new TestRelationships(""), result);
        addTestClassesFromOther(new TestToDatedRelationshipViaColumn(), result);
        addTestClassesFromOther(new TestReadOnlyInherited(), result);
        addTestClassesFromOther(new TestSum(), result);
        addTestClassesFromOther(new TestAvg(), result);
        addTestClassesFromOther(new TestMax(), result);
        addTestClassesFromOther(new TestMin(), result);
        addTestClassesFromOther(new TestCount(), result);
        addTestClassesFromOther(new TestAggregateWithNull(), result);
        addTestClassesFromOther(new TestAggregationWithHavingClause(), result);
        addTestClassesFromOther(new TestAggregateListWithOrderBy(), result);
        addTestClassesFromOther(new TestAggregateBeanListWithPrimitives(), result);
        addTestClassesFromOther(new TestNumericAttribute(), result);
        addTestClassesFromOther(new TestExists(), result);
        addTestClassesFromOther(new TestForceRefresh(), result);
        addTestClassesFromOther(new TestTupleIn(), result);
        addTestClassesFromOther(new TestYearMonthDayOfMonth(), result);
        result.add(User.class);
        result.add(Order.class);
        result.add(OrderItem.class);
        result.add(AuditedOrder.class);
        result.add(AuditedOrderItem.class);
        result.add(AuditedOrderStatus.class);
        result.add(AuditedOrderStatusTwo.class);
        result.add(ParaBalance.class);
        result.add(Book.class);
        result.add(DatedEntity.class);
        result.add(Account.class);
        result.add(Manufacturer.class);
        result.add(Trial.class);
        result.add(ParaDesk.class);
        result.add(Profile.class);
        result.add(Group.class);
        result.add(UserGroup.class);
        result.add(TamsAccount.class);
        result.add(NoExportTestObject.class);
        result.add(AccountTransactionException.class);
        result.add(SpecialAccount.class);
        result.add(FullyCachedTinyBalance.class);
        Class[] array = new Class[result.size()];
        result.toArray(array);
        return array;
    }

    private TestStringLike getTestStringLike()
    {
        return new TestStringLike();
    }

    private TestTupleIn getTestTupleIn()
    {
        return new TestTupleIn();
    }

    private TestCursor getTestCursor()
    {
        return new TestCursor();
    }

    private TestExists getTestExists()
    {
        return new TestExists();
    }

    private TestYearMonthDayOfMonth getTestYearMonthDayOfMonth()
    {
        return new TestYearMonthDayOfMonth();
    }

    private TestReadOnlyInherited getTestReadOnlyInherited()
    {
        return new TestReadOnlyInherited();
    }

    private TestSum getTestSum()
    {
        return new TestSum();
    }

    private TestAvg getTestAvg()
    {
        return new TestAvg();
    }

    private TestMax getTestMax()
    {
        return new TestMax();
    }

    private TestMin getTestMin()
    {
        return new TestMin();
    }

    private TestCount getTestCount()
    {
        return new TestCount();
    }

    private TestAggregateWithNull getTestAggregateWithNull()
    {
        return new TestAggregateWithNull();
    }

    private TestAggregationWithHavingClause getTestAggregationWithHavingClause()
    {
        return new TestAggregationWithHavingClause();
    }

    private TestAggregateListWithOrderBy getAggregateListWithOrderBy()
    {
        return new TestAggregateListWithOrderBy();
    }

    private TestAggregateBeanListWithPrimitives getTestAggregateBeanListWithPrimitives()
    {
        return new TestAggregateBeanListWithPrimitives();
    }

    private TestAggregateBeanList getTestAggregateBeanList()
    {
        return new TestAggregateBeanList();
    }

    private TestAggregateBeanListWithHavingClause getTestAggregateBeanListWithHavingClause()
    {
        return new TestAggregateBeanListWithHavingClause();
    }

    private TestAggregatePrimitiveBeanListWithHavingClause getTestAggregatePrimitiveBeanListWithHavingClause()
    {
        return new TestAggregatePrimitiveBeanListWithHavingClause();
    }

    private TestAggregateBeanListOrderBy getTestAggregateBeanListOrderBy()
    {
        return new TestAggregateBeanListOrderBy();
    }

    private TestAggregateBeanListImmutability getTestAggregateBeanListImmutability()
    {
        return new TestAggregateBeanListImmutability();
    }

    private TestAggregateBeanListForSubclass getTestAggregateBeanListForSubclass()
    {
        return new TestAggregateBeanListForSubclass();
    }

    private TestNumericAttribute getTestNumericAttribute()
    {
        return new TestNumericAttribute();
    }

    private TestForceRefresh getTestForceRefresh()
    {
        return new TestForceRefresh();
    }

    private TestAdhocDeepFetch getTestAdhocDeepFetch()
    {
        return new TestAdhocDeepFetch();
    }

    public void testToOne()
    {
        getTestExists().testToOne();
    }

    public void testToMany()
    {
        getTestExists().testToMany();
    }

    public void testOrAndExists()
    {
        getTestExists().testOrAndExists();
    }

    public void testToOneNotExists()
    {
        getTestExists().testToOneNotExists();
    }

    public void testToManyNotExists()
    {
        getTestExists().testToManyNotExists();
    }

    public void testNoExport()
    {
        try
        {
            NoExportTestObjectFinder.getMithraObjectPortal();
            fail("must not get here");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().contains("forget"));
        }
    }

    public void testYearDate()
    {
        getTestYearMonthDayOfMonth().testYearRetrieval();
    }

    public void testMonthDate()
    {
        getTestYearMonthDayOfMonth().testMonthRetrieval();
    }

    public void testDayOfMonthDate()
    {
        getTestYearMonthDayOfMonth().testDayOfMonthRetrieval();
    }

    public void testYearTimestamp()
    {
        getTestYearMonthDayOfMonth().testTimestampYearRetrieval();
    }

    public void testMonthTimestamp()
    {
        getTestYearMonthDayOfMonth().testTimestampMonthRetrieval();
    }

    public void testDayOfMonthTimestamp()
    {
        getTestYearMonthDayOfMonth().testTimestampDayOfMonthRetrieval();
    }

    public void testNoClientCache()
    {
        AccountTransactionException exp = AccountTransactionExceptionFinder.findOne(AccountTransactionExceptionFinder.deskId().eq("A").and(
                AccountTransactionExceptionFinder.exceptionId().eq(1002)));
        assertNotNull(exp);
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        exp = AccountTransactionExceptionFinder.findOne(AccountTransactionExceptionFinder.deskId().eq("A").and(
                AccountTransactionExceptionFinder.exceptionId().eq(1002)));
        assertTrue(MithraManagerProvider.getMithraManager().getRemoteRetrieveCount() > count);
    }

    public void testFullClientCache()
    {
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        SpecialAccount acc = SpecialAccountFinder.findOne(SpecialAccountFinder.deskId().eq("A").and(
                SpecialAccountFinder.specialAccountId().eq(1)));
        assertNotNull(acc);
        assertEquals(count, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testRemoteDateOperations() throws Exception
    {
        Date date = dateFormat.parse("1900-01-01");
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.closedDate().greaterThan(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getClosedDate().getTime() > date.getTime());
        }
        date = dateFormat.parse("1981-06-08");
        desks = new ParaDeskList(ParaDeskFinder.closedDate().eq(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertEquals(date.getTime(), desk.getClosedDate().getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.closedDate().notEq(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertFalse(date.getTime() == desk.getClosedDate().getTime());
        }

        HashSet set = new HashSet();
        set.add(date);
        set.add(dateFormat.parse("1900-01-01"));
        HashSet copy = new HashSet();
        copy.add(date.getTime());
        copy.add(dateFormat.parse("1900-01-01").getTime());
        desks = new ParaDeskList(ParaDeskFinder.closedDate().in(set));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(set.contains(desk.getClosedDate()));
            copy.remove(desk.getClosedDate().getTime());
        }
        assertTrue(copy.isEmpty());

        desks = new ParaDeskList(ParaDeskFinder.closedDate().notIn(set));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertFalse(set.contains(desk.getClosedDate()));
        }

        desks = new ParaDeskList(ParaDeskFinder.closedDate().greaterThan(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getClosedDate().getTime() > date.getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.closedDate().greaterThanEquals(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getClosedDate().getTime() >= date.getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.closedDate().lessThanEquals(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getClosedDate().getTime() <= date.getTime());
        }

        date = dateFormat.parse("1999-06-06");
        desks = new ParaDeskList(ParaDeskFinder.closedDate().lessThan(date));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getClosedDate().getTime() < date.getTime());
        }

    }

    public void testRemoteTimestampOperations() throws Exception
    {
        Timestamp timestamp = new Timestamp(dateFormat.parse("1900-01-01").getTime());
        ParaDeskList desks = new ParaDeskList(ParaDeskFinder.createTimestamp().greaterThan(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getCreateTimestamp().getTime() > timestamp.getTime());
        }
        timestamp = new Timestamp(timestampFormat.parse("1981-06-08 02:01:00").getTime());
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().eq(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertEquals(timestamp.getTime(), desk.getCreateTimestamp().getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notEq(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertFalse(timestamp.getTime() == desk.getCreateTimestamp().getTime());
        }

        HashSet<Timestamp> set = new HashSet<Timestamp>();
        set.add(timestamp);
        Timestamp secondTimestamp = new Timestamp(timestampFormat.parse("1900-01-01 00:00:00").getTime());
        set.add(secondTimestamp);
        HashSet<Long> copy = new HashSet<Long>();
        copy.add(timestamp.getTime());
        copy.add(secondTimestamp.getTime());
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().in(set));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(set.contains(desk.getCreateTimestamp()));
            copy.remove(desk.getCreateTimestamp().getTime());
        }
        assertTrue(copy.isEmpty());

        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().notIn(set));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertFalse(set.contains(desk.getCreateTimestamp()));
        }

        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().greaterThan(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getCreateTimestamp().getTime() > timestamp.getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().greaterThanEquals(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getCreateTimestamp().getTime() >= timestamp.getTime());
        }

        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().lessThanEquals(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getCreateTimestamp().getTime() <= timestamp.getTime());
        }

        timestamp = new Timestamp(timestampFormat.parse("1999-06-06 00:00:00").getTime());
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().lessThan(timestamp));
        assertTrue(desks.size() > 0);
        for (ParaDesk desk : desks)
        {
            assertTrue(desk.getCreateTimestamp().getTime() < timestamp.getTime());
        }
    }

    public void testByteSum()
    {
        getTestNumericAttribute().testByteSum();
    }

    public void testShortSum()
    {
        getTestNumericAttribute().testShortSum();
    }

    public void testLongSum()
    {
        getTestNumericAttribute().testLongSum();
    }

    public void testFloatSum()
    {
        getTestNumericAttribute().testFloatSum();
    }

    public void testIntDivisionPromotedToDouble()
    {
        getTestNumericAttribute().testIntDivisionPromotedToDouble();
    }

    public void testIntDivisionPromotedToLong()
    {
        getTestNumericAttribute().testIntDivisionPromotedToLong();
    }

    public void testIntDivisionPromotedToFloat()
    {
        getTestNumericAttribute().testIntDivisionPromotedToFloat();
    }

    public void testLongDivisionPromotedToFloat()
    {
        getTestNumericAttribute().testLongDivisionPromotedToFloat();
    }

    public void testLongDivisionPromotedToDouble()
    {
        getTestNumericAttribute().testLongDivisionPromotedToDouble();
    }

    public void testFloatDivisionPromotedToDouble()
    {
        getTestNumericAttribute().testFloatDivisionPromotedToDouble();
    }

    public void testByteDivisionPromotedToDouble()
    {
        getTestNumericAttribute().testByteDivisionPromotedToDouble();
    }

    public void testDoubleDivisionPromotedToBigDecimal()
    {
        getTestNumericAttribute().testDoubleDivisionPromotedToBigDecimal();
    }

    public void testSumOfAggregatingAdditionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testSumOfAggregatingAdditionCalculatedBigDecimalAttribute();
    }

    public void testSumOfSubstractionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testSumOfSubstractionCalculatedBigDecimalAttribute();
    }

    public void testSumOfDivisionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testSumOfDivisionCalculatedBigDecimalAttribute();
    }

    public void testSumOfDivisionCalculatedMappedBigDecimalAttribute()
    {
        getTestNumericAttribute().testSumOfDivisionCalculatedMappedBigDecimalAttribute();
    }

    public void testSumOfMultiplicationCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testSumOfMultiplicationCalculatedBigDecimalAttribute();
    }


    public void testAvgOfAggregatingAdditionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testAvgOfAggregatingAdditionCalculatedBigDecimalAttribute();
    }

    public void testAvgOfSubstractionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testAvgOfSubstractionCalculatedBigDecimalAttribute();
    }

    public void testAvgOfDivisionCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testAvgOfDivisionCalculatedBigDecimalAttribute();
    }

    public void testAvgOfDivisionCalculatedMappedBigDecimalAttribute()
    {
        getTestNumericAttribute().testAvgOfDivisionCalculatedMappedBigDecimalAttribute();
    }

    public void testAvgOfMultiplicationCalculatedBigDecimalAttribute()
    {
        getTestNumericAttribute().testAvgOfMultiplicationCalculatedBigDecimalAttribute();
    }

    public void testDoubleSumWithNull()
    {
        getTestAggregateWithNull().testDoubleSumWithNull();
    }

    public void testIntSumWithNull()
    {
        getTestAggregateWithNull().testIntSumWithNull();
    }

    public void testDoubleAvgWithNull()
    {
        getTestAggregateWithNull().testDoubleAvgWithNull();
    }

    public void testIntAvgWithNull()
    {
        getTestAggregateWithNull().testIntAvgWithNull();
    }

    public void testDateMaxWithNull()
            throws ParseException
    {
        getTestAggregateWithNull().testDateMaxWithNull();
    }

    public void testTimestampMaxWithNull()
            throws ParseException
    {
        getTestAggregateWithNull().testTimestampMaxWithNull();
    }

    public void testStringMaxWithNull()
            throws ParseException
    {
        getTestAggregateWithNull().testStringMaxWithNull();
    }

    public void testBooleanMaxWithNull()
    {
        getTestAggregateWithNull().testBooleanMaxWithNull();
    }

    public void testMappedDoubleSumWithNull()
    {
        getTestAggregateWithNull().testMappedDoubleSumWithNull();
    }

    public void testAdditionCalculatedDoubleSumWithNull()
    {
        getTestAggregateWithNull().testAdditionCalculatedDoubleSumWithNull();
    }

    public void testSubstractionCalculatedDoubleSumWithNull()
    {
        getTestAggregateWithNull().testSubstractionCalculatedDoubleSumWithNull();
    }

    public void testMultiplicationCalculatedDoubleSumWithNull()
    {
        getTestAggregateWithNull().testMultiplicationCalculatedDoubleSumWithNull();
    }

    public void testNullException()
    {
        getTestAggregateWithNull().testNullException();
    }

    public void testStringHavingEq()
    {
        getTestAggregationWithHavingClause().testStringHavingEq();
    }

    public void testStringHavingEqWithDifferentAttribute()
    {
        getTestAggregationWithHavingClause().testStringHavingEqWithDifferentAttribute();
    }

    public void testStringHavingNotEq()
    {
        getTestAggregationWithHavingClause().testStringHavingNotEq();
    }

    public void testStringHavingGreaterThan()
    {
        getTestAggregationWithHavingClause().testStringHavingGreaterThan();
    }

    public void testStringHavingGreaterThanEq()
    {
        getTestAggregationWithHavingClause().testStringHavingGreaterThanEq();
    }

    public void testStringHavingLessThan()
    {
        getTestAggregationWithHavingClause().testStringHavingLessThan();
    }

    public void testStringHavingLessThanEq()
    {
        getTestAggregationWithHavingClause().testStringHavingLessThanEq();
    }

    public void testTimestampHavingEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingEq();
    }

    public void testTimestampHavingNotEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingNotEq();
    }

    public void testTimestampHavingGreaterThan() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingGreaterThan();
    }

    public void testTimestampHavingGreaterThanEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingGreaterThanEq();
    }

    public void testTimestampHavingLessThan() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingLessThan();
    }

    public void testTimestampHavingLessThanEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testTimestampHavingLessThanEq();
    }


    public void testDateHavingEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingEq();
    }

    public void testDateHavingNotEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingNotEq();
    }

    public void testDateHavingGreaterThan() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingGreaterThan();
    }

    public void testDateHavingGreaterThanEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingGreaterThanEq();
    }

    public void testDateHavingLessThan() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingLessThan();
    }

    public void testDateHavingLessThanEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDateHavingLessThanEq();
    }


    public void testBooleanHavingEq()
    {
        getTestAggregationWithHavingClause().testBooleanHavingEq();
    }

    public void testBooleanHavingNotEq()
    {
        getTestAggregationWithHavingClause().testBooleanHavingNotEq();
    }

    public void testBooleanHavingGreaterThan()
    {
        getTestAggregationWithHavingClause().testBooleanHavingGreaterThan();
    }

    public void testBooleanHavingGreaterThanEq()
    {
        getTestAggregationWithHavingClause().testBooleanHavingGreaterThanEq();
    }

    public void testBooleanHavingLessThan()
    {
        getTestAggregationWithHavingClause().testBooleanHavingLessThan();
    }

    public void testBooleanHavingLessThanEq()
    {
        getTestAggregationWithHavingClause().testBooleanHavingLessThanEq();
    }


    public void testCharHavingEq()
    {
        getTestAggregationWithHavingClause().testCharHavingEq();
    }

    public void testCharHavingNotEq()
    {
        getTestAggregationWithHavingClause().testCharHavingNotEq();
    }

    public void testCharHavingGreaterThan()
    {
        getTestAggregationWithHavingClause().testCharHavingGreaterThan();
    }

    public void testCharHavingGreaterThanEq()
    {
        getTestAggregationWithHavingClause().testCharHavingGreaterThanEq();
    }

    public void testCharHavingLessThan()
    {
        getTestAggregationWithHavingClause().testCharHavingLessThan();
    }

    public void testCharHavingLessThanEq()
    {
        getTestAggregationWithHavingClause().testCharHavingLessThanEq();
    }

    public void testIntegerHavingEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testIntegerHavingEq();
    }


    public void testIntegerHavingNotEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testIntegerHavingNotEq();
    }

    public void testIntegerHavingGreaterThan()
    {
        getTestAggregationWithHavingClause().testIntegerHavingGreaterThan();
    }

    public void testIntegerHavingGreaterThanEq()
    {
        getTestAggregationWithHavingClause().testIntegerHavingGreaterThanEq();
    }

    public void testIntegerHavingLessThan()
    {
        getTestAggregationWithHavingClause().testIntegerHavingLessThan();
    }

    public void testIntegerHavingLessThanEq()
    {
        getTestAggregationWithHavingClause().testIntegerHavingLessThanEq();
    }

    public void testDoubleHavingEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDoubleHavingEq();
    }


    public void testDoubleHavingNotEq() throws ParseException
    {
        getTestAggregationWithHavingClause().testDoubleHavingNotEq();
    }

    public void testDoubleHavingGreaterThan()
    {

        getTestAggregationWithHavingClause().testDoubleHavingGreaterThan();
    }

    public void testDoubleHavingGreaterThanEq()
    {
        getTestAggregationWithHavingClause().testDoubleHavingGreaterThanEq();
    }

    public void testDoubleHavingLessThan()
    {
        getTestAggregationWithHavingClause().testDoubleHavingLessThan();
    }

    public void testDoubleHavingLessThanEq()
    {
        getTestAggregationWithHavingClause().testDoubleHavingLessThanEq();
    }


    public void testCountAggregationWithHaving()
    {
        getTestAggregationWithHavingClause().testCountAggregationWithHaving();
    }

    public void testMappedDoubleHavingEq()
    {
        getTestAggregationWithHavingClause().testMappedDoubleHavingEq();
    }

    public void testMappedDoubleHavingEqUsingDifferentAttributeInHaving()
    {
        getTestAggregationWithHavingClause().testMappedDoubleHavingEqUsingDifferentAttributeInHaving();
    }


    public void testDatedMappedDoubleHavingEqUsingDifferentAttributeInHaving()
    {
        getTestAggregationWithHavingClause().testDatedMappedDoubleHavingEqUsingDifferentAttributeInHaving();
    }

    public void testAndHavingOperation()
    {
        getTestAggregationWithHavingClause().testAndHavingOperation();
    }

    public void testOrHavingOperation()
    {

        getTestAggregationWithHavingClause().testOrHavingOperation();
    }

    public void testComplexHavingOperation()
    {
        getTestAggregationWithHavingClause().testComplexHavingOperation();
    }


    public void testCountAllOperationAndRegularAggregateAttribute()
    {
        getTestCount().testAllOperationAndRegularAggregateAttribute();
    }

    public void testCountNotMappedOperationAndAggregateAttribute()
    {
        getTestCount().testNotMappedOperationAndAggregateAttribute();
    }

    public void testCountMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        getTestCount().testMappedToOneOperationAndNotMappedAggregateAttribute();
    }

    public void testCountAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        getTestCount().testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute();
    }

    public void testCountNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        getTestCount().testNotMappedOperationAndMappedToManyAggregateAttribute();
    }

    public void testCountMappedToManyOperationAndAggregateAttribute()
    {
        getTestCount().testMappedToManyOperationAndAggregateAttribute();
    }

    public void testCountOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestCount().testOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testCountAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestCount().testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testCountAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        getTestCount().testAndOperationWithDifferentMappersMappedToManyAttribute();
    }

    public void testCountToOneOperationAndMappedToManyAttribute()
    {
        getTestCount().testToOneOperationAndMappedToManyAttribute();
    }

    public void testCountOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        getTestCount().testOperationWithLinkedToOneMapperAndToManyAggregateAttribute();
    }

    public void testCountAllOperationAndCalculatedAggregateAttribute()
    {
        getTestCount().testAllOperationAndCalculatedAggregateAttribute();
    }

    public void testCountNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestCount().testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testCountMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestCount().testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testCountMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestCount().testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testCountNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestCount().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testCountMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestCount().testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testCountMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestCount().testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testCountAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestCount().testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testCountAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestCount().testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testCountToOneRelationshipInBothOpAndAggregate()
    {
        getTestCount().testToOneRelationshipInBothOpAndAggregate();
    }

    public void testCountToManyRelationshipThatBecomesToOne()
    {
        getTestCount().testToManyRelationshipThatBecomesToOne();
    }

    public void testCountTimestamp()
            throws ParseException
    {
        getTestCount().testCountTimestamp();
    }

    public void testCountDate()
            throws ParseException
    {
        getTestCount().testCountDate();
    }

    public void testCountBoolean()
            throws ParseException
    {
        getTestCount().testCountBoolean();
    }

    public void testCountString()
            throws ParseException
    {
        getTestCount().testCountString();
    }

    public void testMinAllOperationAndRegularAggregateAttribute()
    {
        getTestMin().testAllOperationAndRegularAggregateAttribute();
    }

    public void testMinNotMappedOperationAndAggregateAttribute()
    {
        getTestMin().testNotMappedOperationAndAggregateAttribute();
    }

    public void testMinMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        getTestMin().testMappedToOneOperationAndNotMappedAggregateAttribute();
    }

    public void testMinAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        getTestMin().testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute();
    }

    public void testMinNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        getTestMin().testNotMappedOperationAndMappedToManyAggregateAttribute();
    }

    public void testMinMappedToManyOperationAndAggregateAttribute()
    {
        getTestMin().testMappedToManyOperationAndAggregateAttribute();
    }

    public void testMinOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestMin().testOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testMinAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestMin().testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testMinAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        getTestMin().testAndOperationWithDifferentMappersMappedToManyAttribute();
    }

    public void testMinToOneOperationAndMappedToManyAttribute()
    {
        getTestMin().testToOneOperationAndMappedToManyAttribute();
    }

    public void testMinOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        getTestMin().testOperationWithLinkedToOneMapperAndToManyAggregateAttribute();
    }

    public void testMinAllOperationAndCalculatedAggregateAttribute()
    {
        getTestMin().testAllOperationAndCalculatedAggregateAttribute();
    }

    public void testMinNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMin().testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMinMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMin().testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMinMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMin().testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMinNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMin().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMinMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMin().testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMinMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMin().testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMinAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMin().testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMinAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMin().testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMinToOneRelationshipInBothOpAndAggregate()
    {
        getTestMin().testToOneRelationshipInBothOpAndAggregate();
    }

    public void testMinToManyRelationshipThatBecomesToOne()
    {
        getTestMin().testToManyRelationshipThatBecomesToOne();
    }

    public void testMinTimestamp()
            throws ParseException
    {
        getTestMin().testMinTimestamp();
    }

    public void testMinDate()
            throws ParseException
    {
        getTestMin().testMinDate();
    }

    public void testMinBoolean()
    {
        getTestMin().testMinBoolean();
    }

    public void testMinString()
            throws ParseException
    {
        getTestMin().testMinString();
    }

    public void testMaxAllOperationAndRegularAggregateAttribute()
    {
        getTestMax().testAllOperationAndRegularAggregateAttribute();
    }

    public void testMaxNotMappedOperationAndAggregateAttribute()
    {
        getTestMax().testNotMappedOperationAndAggregateAttribute();
    }

    public void testMaxMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        getTestMax().testMappedToOneOperationAndNotMappedAggregateAttribute();
    }

    public void testMaxAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        getTestMax().testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute();
    }

    public void testMaxNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        getTestMax().testNotMappedOperationAndMappedToManyAggregateAttribute();
    }

    public void testMaxMappedToManyOperationAndAggregateAttribute()
    {
        getTestMax().testMappedToManyOperationAndAggregateAttribute();
    }

    public void testMaxOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestMax().testOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testMaxAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestMax().testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testMaxAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        getTestMax().testAndOperationWithDifferentMappersMappedToManyAttribute();
    }

    public void testMaxToOneOperationAndMappedToManyAttribute()
    {
        getTestMax().testToOneOperationAndMappedToManyAttribute();
    }

    public void testMaxOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        getTestMax().testOperationWithLinkedToOneMapperAndToManyAggregateAttribute();
    }

    public void testMaxAllOperationAndCalculatedAggregateAttribute()
    {
        getTestMax().testAllOperationAndCalculatedAggregateAttribute();
    }

    public void testMaxNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMax().testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMaxMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMax().testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMaxMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestMax().testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMaxNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMax().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMaxMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMax().testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMaxMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMax().testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMaxAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMax().testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMaxAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestMax().testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMaxToOneRelationshipInBothOpAndAggregate()
    {
        getTestMax().testToOneRelationshipInBothOpAndAggregate();
    }

    public void testMaxToManyRelationshipThatBecomesToOne()
    {
        getTestMax().testToManyRelationshipThatBecomesToOne();
    }

    public void testMaxTimestamp()
            throws ParseException
    {
        getTestMax().testMaxTimestamp();
    }

    public void testMaxDate()
            throws ParseException
    {
        getTestMax().testMaxDate();
    }

    public void testMaxBoolean()
    {
        getTestMax().testMaxBoolean();
    }

    public void testMaxMappedTimestamp()
            throws ParseException
    {
        getTestMax().testMaxMappedTimestamp();
    }

    public void testMaxMappedDate()
            throws ParseException
    {
        getTestMax().testMaxMappedDate();
    }

    public void testMaxMappedBoolean()
    {
        getTestMax().testMaxMappedBoolean();
    }

    public void testMaxString()
            throws ParseException
    {
        getTestMax().testMaxString();
    }

    public void testMaxMappedString()
    {
        getTestMax().testMaxMappedString();
    }

    public void testAvgAllOperationAndRegularAggregateAttribute()
    {
        getTestAvg().testAllOperationAndRegularAggregateAttribute();
    }

    public void testNotMappedOperationAndAggregateAttribute()
    {
        getTestAvg().testNotMappedOperationAndAggregateAttribute();
    }

    public void testMappedToOneOperationAndNotMappedAggregateAttribute()
    {
        getTestAvg().testMappedToOneOperationAndNotMappedAggregateAttribute();
    }

    public void testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute()
    {
        getTestAvg().testAndOperationWithMappedToOneAttributeAndNotMappedAggregateAttribute();
    }

    public void testNotMappedOperationAndMappedToManyAggregateAttribute()
    {
        getTestAvg().testNotMappedOperationAndMappedToManyAggregateAttribute();
    }

    public void testMappedToManyOperationAndAggregateAttribute()
    {
        getTestAvg().testMappedToManyOperationAndAggregateAttribute();
    }

    public void testOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestAvg().testOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute()
    {
        getTestAvg().testAndOperationWithLinkedToOneMapperAndMappedToManyAttribute();
    }

    public void testAvgAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        getTestAvg().testAndOperationWithDifferentMappersMappedToManyAttribute();
    }

    public void testAvgToOneOperationAndMappedToManyAttribute()
    {
        getTestAvg().testToOneOperationAndMappedToManyAttribute();
    }

    public void testOperationWithLinkedToOneMapperAndToManyAggregateAttribute()
    {
        getTestAvg().testOperationWithLinkedToOneMapperAndToManyAggregateAttribute();
    }

    public void testAvgAllOperationAndCalculatedAggregateAttribute()
    {
        getTestAvg().testAllOperationAndCalculatedAggregateAttribute();
    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestAvg().testNotMappedOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testAvgMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestAvg().testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testAvgMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestAvg().testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testAvgNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestAvg().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAvgMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestAvg().testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAvgMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestAvg().testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAvgAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestAvg().testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAvgAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestAvg().testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAvgToOneRelationshipInBothOpAndAggregate()
    {
        getTestAvg().testToOneRelationshipInBothOpAndAggregate();
    }

    public void testAvgToManyRelationshipThatBecomesToOne()
    {
        getTestAvg().testToManyRelationshipThatBecomesToOne();
    }

    public void testAllOperationAndRegularAggregateAttribute()
    {
        getTestSum().testAllOperationAndRegularAggregateAttribute();
    }

    public void testRegularOperationAndAggregateAttribute()
    {
        getTestSum().testRegularOperationAndAggregateAttribute();
    }

    public void testMappedToOneOpAndRegularAggregateAttribute()
    {
        getTestSum().testMappedToOneOpAndRegularAggregateAttribute();
    }

    public void testAndOperationWithToOneOpAndRegularAggregateAttribute()
    {
        getTestSum().testAndOperationWithToOneOpAndRegularAggregateAttribute();
    }

    public void testRegularOpAndMappedToManyAggregateAttribute()
    {
        getTestSum().testRegularOpAndMappedToManyAggregateAttribute();
    }

    public void testToManyOpAndAggregateAttribute()
    {
        getTestSum().testToManyOpAndAggregateAttribute();
    }

    public void testOpWithLinkedToManyAndMappedToManyAttribute()
    {
        getTestSum().testOpWithLinkedToManyAndMappedToManyAttribute();
    }

    public void testAndOperationWithLinkedToManyMapperAndMappedToManyAttribute()
    {
        getTestSum().testAndOperationWithLinkedToManyMapperAndMappedToManyAttribute();
    }

    public void testAndOperationWithDifferentMappersMappedToManyAttribute()
    {
        getTestSum().testAndOperationWithDifferentMappersMappedToManyAttribute();
    }

    public void testToOneOperationAndMappedToManyAttribute()
    {
        getTestSum().testToOneOperationAndMappedToManyAttribute();
    }

    public void testOpWithLinkedToManyMapperAndToManyAggregateAttribute()
    {
        getTestSum().testOpWithLinkedToManyMapperAndToManyAggregateAttribute();
    }

    public void testAllOperationAndCalculatedAggregateAttribute()
    {
        getTestSum().testAllOperationAndCalculatedAggregateAttribute();
    }

    public void testRegularOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestSum().testRegularOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestSum().testMappedToOneOperationAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper()
    {
        getTestSum().testMappedOperationWithDifferentMappersAndCalculatedAggregateAttributeWithToOneMapper();
    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestSum().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper2()
    {
        getTestSum().testNotMappedOperationAndCalculatedAggregateAttributeWithLinkedMapper2();
    }

    public void testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestSum().testMappedToManyOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestSum().testMappedToOneOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestSum().testAndOperationAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper()
    {
        getTestSum().testAndOperationWithDifferentMappersAndCalculatedAggregateAttributeWithLinkedMapper();
    }

    public void testToOneRelationshipInBothOpAndAggregate()
    {
        getTestSum().testToOneRelationshipInBothOpAndAggregate();
    }

    public void testToManyRelationshipThatBecomesToOne()
    {
        getTestSum().testToManyRelationshipThatBecomesToOne();
    }

    public void test3()
    {
        getTestSum().test3();
    }

    public void test7()
    {
        getTestSum().test7();
    }

    public void testTestTest()
    {
        getTestSum().testTestTest();
    }

    public void testYear()
    {
        getTestSum().testSumDateYear();
    }

    public void testAllOpWithCalculatedAggrAttrWithLinkedMapperWithMoreThanOneToManyMappers()
    {
        getTestSum().testAllOpWithCalculatedAggrAttrWithLinkedMapperWithMoreThanOneToManyMappers();
    }

    public void testAllOpWithCalculatedAggrAttrWithSingleAttributeAndLinkedMapper()
    {
        getTestSum().testAllOpWithCalculatedAggrAttrWithSingleAttributeAndLinkedMapper();
    }

    public void testMultipleGroupByAttributes()
    {
        getTestSum().testMultipleGroupByAttributes();
    }

    public void testMultipleAggregateattributes()
    {
        getTestSum().testMultipleAggregateattributes();
    }

    public void testcase1_7()
    {
        getTestSum().testcase1_7();
    }

    public void testcase1_8()
    {
        getTestSum().testcase1_8();
    }

    public void testSubstraction()
    {
        getTestSum().testSubstraction();
    }

    public void testSubstractionWithMappedToManyAggregateAttribute()
    {
        getTestSum().testSubstractionWithMappedToManyAggregateAttribute();
    }

    public void testAddition()
    {
        getTestSum().testAddition();
    }

    public void testAdditionWithMappedToManyAttributes()
    {
        getTestSum().testAdditionWithMappedToManyAttributes();
    }

    public void testDoubleDivision()
    {
        getTestSum().testDoubleDivision();
    }

    public void testIntDivision()
    {
        getTestSum().testIntDivision();
    }

    public void testReadMonkeyById()
    {
        getTestReadOnlyInherited().testReadMonkeyById();
    }

    public void testReadMonkeyByName()
    {
        getTestReadOnlyInherited().testReadMonkeyByName();
    }

    public void testReadMonkeyByBodyTemp()
    {
        getTestReadOnlyInherited().testReadMonkeyByBodyTemp();
    }

    public void testReadMonkeyByBodyTailLength()
    {
        getTestReadOnlyInherited().testReadMonkeyByBodyTailLength();
    }

    public void testPolymorphicAnimalById()
    {
        getTestReadOnlyInherited().testPolymorphicAnimalById();
    }

    public void testPolymorphicAnimal()
    {
        getTestReadOnlyInherited().testPolymorphicAnimal();
    }

    public void testPolymorphicMammalById()
    {
        getTestReadOnlyInherited().testPolymorphicMammalById();
    }

    public void testUniquenessTopFirst()
    {
        getTestReadOnlyInherited().testUniquenessTopFirst();
    }

    public void testUniquenessBottomFirst()
    {
        getTestReadOnlyInherited().testUniquenessBottomFirst();
    }

    public void testComputeFunction()
    {
        Operation operation = UserFinder.active().eq(true).and(UserFinder.sourceId().eq(0));
        List results = UserFinder.getMithraObjectPortal().computeFunction(operation, null,
                "max(" + UserFinder.id().getColumnName() + ")", new IntegerResultSetParser());
        assertEquals(1, results.size());
        assertEquals(50, ((Integer) results.get(0)).intValue());
    }

    public void testCount()
    {
        UserList list = new UserList(UserFinder.active().eq(true).and(UserFinder.sourceId().eq(0)));
        int count = list.count();
        assertEquals(count, list.size());
    }

    public void testDatedOrderBy()
    {
        TestOrderby test = new TestOrderby();
        test.testDatedOrderBy();
    }

    public void testSimpleOperation() throws Exception
    {
        OrderList orders = new OrderList(OrderFinder.orderId().eq(1));
        assertEquals(1, orders.size());
        // 1,"2004-01-12 00:00:00.0", 1, "First order", "In-Progress", "123"
        Order order = orders.get(0);
        assertEquals(1, order.getOrderId());
        assertEquals("First order", order.getDescription());
        assertEquals("In-Progress", order.getState());
    }

    public void testDeepFetchRelationship() throws Exception
    {
        TestAdditionalRelationships testAdditionalRelationships = new TestAdditionalRelationships("testDeepFetchRelationship");
        testAdditionalRelationships.testDeepFetchRelationship();
    }

    public void testParent() throws Exception
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testParent();
    }

    public void testParentWithNoteParent()
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testParentWithNoteParent();
    }

    public void testParentWithSameOtherTimestamp()
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testParentWithSameOtherTimestamp();
    }

    public void testChildren() throws Exception
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testChildren();
    }

    public void testNoParent()
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testNoParent();
    }

    public void testSelfJoinWithDualConstant()
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testSelfJoinWithDualConstant();
    }

    public void testSelfJoinWithDualConstantDeepFetch()
    {
        SelfJoinTest selfJoinTest = new SelfJoinTest();
        selfJoinTest.testSelfJoinWithDualConstantDeepFetch();
    }

    public void testThreeLayerRelationship()
            throws Exception
    {
        TestAdditionalRelationships testAdditionalRelationships = new TestAdditionalRelationships("foo");
        testAdditionalRelationships.testThreeLayerRelationship();
    }

    public void testDeepFetchRelationship2()
            throws Exception
    {
        TestAdditionalRelationships testAdditionalRelationships = new TestAdditionalRelationships("foo");
        testAdditionalRelationships.testDeepFetchRelationship();
    }

    public void testAllQuery()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testAllQuery();
    }

    public void testTamsAccountInfinityRetrieval()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testTamsAccountInfinityRetrieval();
    }

    public void testTamsAccountOr()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testTamsAccountOr();
    }

    public void testDatedAccountInfinityRetreival()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testDatedAccountInfinityRetreival();
    }


    public void testDeepFetchOfDatedRelationships()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testDeepFetchOfDatedRelationships();
    }

    public void testTwoLevelDeepDatedRelationship()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testTwoLevelDeepDatedRelationship();
    }

    public void testFinderWithRelationshipToDated()
    {
        TestDatedBasicRetrieval testDatedBasicRetrieval = new TestDatedBasicRetrieval();
        testDatedBasicRetrieval.testFinderWithRelationshipToDated();
    }

    public void testEqWithBooleanAttribute()
            throws Exception
    {
        TestBasicBooleanOperation test = new TestBasicBooleanOperation();
        test.testEqWithBooleanAttribute();
    }

    public void testNotEqWithBooleanAttribute()
            throws Exception
    {
        TestBasicBooleanOperation test = new TestBasicBooleanOperation();
        test.testNotEqWithBooleanAttribute();
    }

    public void testEqWithByteAttribute()
            throws Exception
    {
        TestBasicByteOperation test = new TestBasicByteOperation();
        test.testEqWithByteAttribute();
    }

    public void testEmptyInOperation()
            throws SQLException
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testEmptyInOperation();
    }

    public void testNoOperation()
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testNoOperation();
    }

    public void testTrimString()
            throws SQLException
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testTrimString();
    }

    public void testNullablePrimitiveAttributes()
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testNullablePrimitiveAttributes();
    }

    public void testGetAttributeByName()
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testGetAttributeByName();
    }

    public void testDeepFetchOneToManyWithoutSourceId()
    {
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        OrderList order = new OrderList(OrderFinder.orderId().eq(1));
        order.deepFetch(OrderFinder.items());
        order.forceResolve();
        assertTrue(MithraManagerProvider.getMithraManager().getRemoteRetrieveCount() - count > 1);
    }

    public void testDeepFetchManyToOneWithoutSourceId()
    {
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        BookList books = new BookList(BookFinder.inventoryId().eq(1));
        books.deepFetch(BookFinder.manufacturer());
        books.forceResolve();
        assertTrue(MithraManagerProvider.getMithraManager().getRemoteRetrieveCount() - count > 1);
    }

    public void testClearReadOnlyCache()
    {
        if (TrialFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            String trialId = "001A";

            Trial trial = TrialFinder.findOne(TrialFinder.trialId().eq(trialId));
            assertNotNull(trial);
            int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
            assertSame(trial, TrialFinder.findOne(TrialFinder.trialId().eq(trialId)));
            assertEquals(count, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
            TrialFinder.clearQueryCache();
            assertSame(trial, TrialFinder.findOne(TrialFinder.trialId().eq(trialId)));
            assertTrue(count < MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
        }
    }

    public void testMaxObjectsToRetrieve()
    {
        TestBasicRetrieval test = new TestBasicRetrieval();
        test.testMaxObjectsToRetrieve();
    }

    public void testRetrieveOneRowAndCache()
    {
        TestBasicTransactionalRetrieval test = new TestBasicTransactionalRetrieval();
        test.testRetrieveOneRowAndCache();
    }

    public void testAbsoluteValueRetrieval()
            throws Exception
    {
        ParaDeskList desks;

        double d = 10.54;
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().eq(d));
        assertEquals(2, desks.size());
        assertEquals(d, Math.abs(desks.getParaDeskAt(0).getSizeDouble()), 0.0);
        assertEquals(d, Math.abs(desks.getParaDeskAt(1).getSizeDouble()), 0.0);

        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().absoluteValue().lessThan(43.23));
        assertTrue(desks.size() > 0);
        for (int i = 0; i < desks.size(); i++)
        {
            assertTrue(Math.abs(desks.getParaDeskAt(i).getSizeDouble()) < 43.23);
        }
    }

    public void testMultiEqualityWithIn()
    {
        User u1 = UserFinder.findOne(UserFinder.sourceId().eq(0).and(UserFinder.id().eq(1)));
        assertNotNull(u1);
        User u2 = UserFinder.findOne(UserFinder.sourceId().eq(0).and(UserFinder.id().eq(2)));
        assertNotNull(u2);
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        IntHashSet set = new IntHashSet();
        set.add(1);
        set.add(2);
        UserList users = new UserList(UserFinder.sourceId().eq(0).and(UserFinder.id().in(set)));
        users.forceResolve();
        assertEquals(2, users.size());
        assertTrue(users.contains(u1));
        assertTrue(users.contains(u2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testDatedNotDatedJoin()
            throws Exception
    {
        TestDatedWithNotDatedJoin test = new TestDatedWithNotDatedJoin();
        test.testDatedNotDatedJoin();
    }

    public void testEqualityAfterMapping()
    {
        TestMappedOperation test = new TestMappedOperation();
        test.testEqualityAfterMapping();
    }

    public void testManyToOne()
    {
        TestRelationships test = new TestRelationships("");
        test.testManyToOne();
    }

    public void testCyclicDependency()
    {
        TestRelationships test = new TestRelationships("");
        test.testCyclicDependency();
    }

    public void testSelfJoinThroughRelationshipTable()
    {
        TestRelationships test = new TestRelationships("");
        test.testSelfJoinThroughRelationshipTable();
    }

    public void testQueryCache()
    {
        TestRelationships test = new TestRelationships("");
        test.testQueryCache();
    }

    public void testChainedRelationship() throws SQLException
    {
        TestRelationships test = new TestRelationships("");
        test.testChainedRelationship();
    }

    public void testParametrizedRelationship()
    {
        TestRelationships test = new TestRelationships("");
        test.testParametrizedRelationship();
    }

    public void testNullAttributeInRelationship()
    {
        TestRelationships test = new TestRelationships("");
        test.testNullAttributeInRelationship();
    }

    public void testSelfJoinWithConstant1()
    {
        TestRelationships test = new TestRelationships("");
        test.testSelfJoinWithConstant1();
    }

    public void testSelfJoinWithConstant2()
    {
        TestRelationships test = new TestRelationships("");
        test.testSelfJoinWithConstant2();
    }

    public void testSelfJoinWithConstant3()
    {
        TestRelationships test = new TestRelationships("");
        test.testSelfJoinWithConstant3();
    }

    public void testCollapsedDeepFetch()
    {
        TestRelationships test = new TestRelationships("");
        test.testCollapsedDeepFetch();
    }

    public void testFlippedDeepFetch()
    {
        TestRelationships test = new TestRelationships("");
        test.testFlippedDeepFetch();
    }

    public void testFlippedDeepFetchWithDeepQuery()
    {
        TestRelationships test = new TestRelationships("");
        test.testFlippedDeepFetchWithDeepQuery();
    }

    public void testNoneCacheDeepFetch()
    {
        TestRelationships test = new TestRelationships("");
        test.testNoneCacheDeepFetch();
    }

    public void testImplicitJoin()
    {
        TestRelationships test = new TestRelationships("");
        test.testImplicitJoin();
    }

    public void testManyToMany() throws SQLException
    {
        User user = UserFinder.findOne(UserFinder.userId().eq("suklaa").and(UserFinder.sourceId().eq(0)));
        GroupList groups = user.getGroups();
        for (int i = 0; i < groups.size(); i++)
        {
            Group group = groups.getGroupAt(i);
            assertTrue(group.getUsers().contains(user));
        }
    }

    public void testDeepFetchToOne()
    {
        UserList users = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.profile());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            assertNotNull(user.getProfile());
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testDeepFetchToOneToMany()
    {
        UserList users = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.profile().users());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            user.getProfile().getUsers().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testDeepFetchToOneWithPreload()
    {
        ProfileList profiles = new ProfileList(ProfileFinder.all().and(ProfileFinder.sourceId().eq(0)));
        profiles.forceResolve();
        UserList users = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.profile());
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        users.forceResolve();
        for (User user : users)
        {
            assertNotNull(user.getProfile());
        }
        assertEquals(oldCount + 1, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testDeepFetchOneToMany()
    {
        GroupList groups = new GroupList(GroupFinder.id().lessThan(3).and(GroupFinder.sourceId().eq(0)));
        groups.deepFetch(GroupFinder.defaultUsers());
        groups.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (Group group : groups)
        {
            group.getDefaultUsers().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testFakeManyToManyRelationshipDeepFetch() throws SQLException
    {
        UserList users = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.userGroups().group());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            user.getGroups().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

    }

    public void testFakeManyToManyRelationshipDeepFetchNegativeCaching() throws SQLException
    {
        UserList users = new UserList(UserFinder.id().eq(50).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.userGroups().group());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            user.getGroups().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

    }

    public void testFakeManyToManyRelationshipDeepFetch2() throws SQLException
    {
        UserList users = new UserList(UserFinder.id().lessThan(5).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.userGroups2().group());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            user.getGroups2().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

    }

    public void testFakeManyToManyRelationshipDeepFetchNegativeCaching2() throws SQLException
    {
        UserList users = new UserList(UserFinder.id().eq(2).and(UserFinder.sourceId().eq(0)));
        users.deepFetch(UserFinder.userGroups2().group());
        users.forceResolve();
        int oldCount = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (User user : users)
        {
            user.getGroups2().forceResolve();
        }
        assertEquals(oldCount, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());

    }

    public void testParametrizedRelationshipDeepFetch()
    {
        OrderList orders = new OrderList(OrderFinder.userId().eq(1));
        orders.deepFetch(OrderFinder.itemForProduct(1));
        orders.forceResolve();
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        for (int i = 0; i < orders.size(); i++)
        {
            Order o = orders.getOrderAt(i);
            o.getItemForProduct(1);
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testParametrizedWithNonEqualityRelationshipDeepFetch()
    {
        OrderList orders = new OrderList(OrderFinder.orderId().eq(2));
        orders.deepFetch(OrderFinder.expensiveItems(11.0));
        orders.forceResolve();
        int count = MithraManagerProvider.getMithraManager().getRemoteRetrieveCount();
        assertEquals(1, orders.size());
        Order order = orders.getOrderAt(0);
        OrderItemList expensiveOrderItems = order.getExpensiveItems(11.0);
        for (int i = 0; i < expensiveOrderItems.size(); i++)
        {
            assertTrue(expensiveOrderItems.getOrderItemAt(i).getOriginalPrice() >= 11.0);
        }
        assertEquals(count, MithraManagerProvider.getMithraManager().getRemoteRetrieveCount());
    }

    public void testIntegerNotEquality() throws Exception
    {
        TestSelfNotEquality test = new TestSelfNotEquality();
        test.testIntegerNotEquality();
    }

    public void testRelatedNotEquality() throws Exception
    {
        TestSelfNotEquality test = new TestSelfNotEquality();
        test.testRelatedNotEquality();
    }

    public void testDoubleSelfEquality() throws Exception
    {
        TestFilterEquality test = new TestFilterEquality();
        test.testDoubleSelfEquality();
    }

    public void testRelatedDoubleEquality() throws Exception
    {
        TestFilterEquality test = new TestFilterEquality();
        test.testRelatedDoubleEquality();
    }

    public void testRelationship()
    {
        TestToDatedRelationshipViaColumn test = new TestToDatedRelationshipViaColumn();
        test.testRelationship();
    }

    public void testDeepFetch()
    {
        TestToDatedRelationshipViaColumn test = new TestToDatedRelationshipViaColumn();
        test.testDeepFetch();
    }

    public void testFindByRelated()
    {
        TestToDatedRelationshipViaColumn test = new TestToDatedRelationshipViaColumn();
        test.testFindByRelated();
    }

    public void testFindByRelatedWithoutDeepFetch()
    {
        TestToDatedRelationshipViaColumn test = new TestToDatedRelationshipViaColumn();
        test.testFindByRelatedWithoutDeepFetch();
    }


    public void testChangingData() throws Exception
    {
        Timestamp asOf = new Timestamp(System.currentTimeMillis());
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        this.getRemoteSlaveVm().executeMethod("serverUpdatePnlGroupOnTamsAccount");
        TamsAccountList tal = new TamsAccountList(TamsAccountFinder.deskId().eq("A").and(TamsAccountFinder.businessDate().eq(asOf)));
        tal.forceResolve();
        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("test", tamsAccount.getPnlGroupId());

    }

    public void serverUpdatePnlGroupOnTamsAccount()
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "update TAMS_ACCOUNT set PNLGROUP_ID = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, "test");
        ps.setString(2, "7410161001");
        int updatedRows = ps.executeUpdate();
        assertEquals(1, updatedRows);
        con.close();
    }

    public void testSettingBusinessToDate() throws Exception
    {
        long time = System.currentTimeMillis() - 10000;
        Timestamp asOf = new Timestamp(time);
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        this.getRemoteSlaveVm().executeMethod("serverUpdatePnlGroupAndBusinessToDate");

        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("test", tamsAccount.getPnlGroupId());
    }

    public void serverUpdatePnlGroupAndBusinessToDate()
            throws SQLException
    {
        long time = System.currentTimeMillis() - 500;
        Connection con = this.getServerSideConnection();
        String sql = "update TAMS_ACCOUNT set PNLGROUP_ID = ?, THRU_Z = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, "test");
        ps.setTimestamp(2, new Timestamp(time + 1000));
        ps.setString(3, "7410161001");
        int updatedRows = ps.executeUpdate();
        con.close();
        assertEquals(1, updatedRows);
    }

    public void testSettingProcessingToDate() throws Exception
    {
        long time = System.currentTimeMillis() - 10000;
        Timestamp asOf = new Timestamp(time);
        TamsAccount tamsAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        this.getRemoteSlaveVm().executeMethod("serverChainInNewTamsAccount");

        tamsAccount = TamsAccountFinder.findOneBypassCache(TamsAccountFinder.accountNumber().eq("7410161001").and(
                TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        assertNotNull(tamsAccount);
        assertEquals("testPnlGroupId", tamsAccount.getPnlGroupId());
    }

    public void serverChainInNewTamsAccount()
            throws SQLException
    {
        Connection con = this.getServerSideConnection();
        String sql = "update TAMS_ACCOUNT set OUT_Z = ? where ACCOUNT_NUMBER = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        Timestamp now = new Timestamp(System.currentTimeMillis());
        ps.setTimestamp(1, now);
        ps.setString(2, "7410161001");
        int updatedRows = ps.executeUpdate();
        ps.close();

        sql = "insert into TAMS_ACCOUNT (ACCOUNT_NUMBER, ACCT_8_DIG_C,TRIAL_ID,PNLGROUP_ID,FROM_Z,THRU_Z,IN_Z,OUT_Z) values (?, ?, ?, ?, ?, ?, ?, ?)";
        ps = con.prepareStatement(sql);
        ps.setString(1, "7410161001");
        ps.setString(2, "74101610");
        ps.setString(3, "testTrialId");
        ps.setString(4, "testPnlGroupId");
        ps.setTimestamp(5, new Timestamp(System.currentTimeMillis() - 1000000));
        ps.setTimestamp(6, InfinityTimestamp.getParaInfinity());
        ps.setTimestamp(7, now);
        ps.setTimestamp(8, InfinityTimestamp.getParaInfinity());
        updatedRows += ps.executeUpdate();
        con.close();
        assertEquals(2, updatedRows);
    }

    public void testCalculatedStringToLower()
    {
        TestCalculatedString testCalculatedString = new TestCalculatedString();
        testCalculatedString.testToLower();
    }

    public void testForceRefreshBitemporalMultiPkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshBitemporalMultiPkList();
    }

    public void testForceRefreshBitemporalSinglePkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshBitemporalSinglePkList();
    }

    public void testForceRefreshHugeNonDatedMultiPkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshHugeNonDatedMultiPkList();
    }

    public void testForceRefreshHugeNonDatedSimpleList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshHugeNonDatedSimpleList();
    }

    public void testForceRefreshHugeSingleDatedMultiPkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshHugeSingleDatedMultiPkList();
    }

    public void testForceRefreshNonDatedMultiPkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshNonDatedMultiPkList();
    }

    public void testForceRefreshNonDatedSimpleList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshNonDatedSimpleList();
    }

    public void testForceRefreshNonDatedSimpleListInTransaction()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshNonDatedSimpleListInTransaction();
    }

    public void testForceRefreshOpBasedList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshOpBasedList();
    }

    public void testForceRefreshSingleDatedMultiPkList()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshSingleDatedMultiPkList();
    }

    public void testForceRefreshHugeNonDatedMultiPkListInTransaction()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshHugeNonDatedMultiPkListInTransaction();
    }

    public void testForceRefreshBitemporalMultiPkListInTransaction()
            throws SQLException
    {
        getTestForceRefresh().testForceRefreshBitemporalMultiPkListInTransaction();
    }

    public void testAdhocDeepFetchOneToManyOneToOneSameAttribute()
    {
        getTestAdhocDeepFetch().testOneToManyOneToOneSameAttribute();
    }

    public void testAdhocDeepFetchOneToManyOneToOneSameAttributeJustOne()
    {
        getTestAdhocDeepFetch().testOneToManyOneToOneSameAttributeJustOne();
    }

    public void testAdhocDeepFetchOneToManyOneToOneSameAttributeDated()
    {
        getTestAdhocDeepFetch().testOneToManyOneToOneSameAttributeDated();
    }

    public void testAdhocDeepFetchOneToManyOneToOneSameAttributeDatedMultiTime()
    {
        getTestAdhocDeepFetch().testOneToManyOneToOneSameAttributeDatedMultiTime();
    }

    public void testAdhocDeepFetchOneToOneWithSource()
    {
        getTestAdhocDeepFetch().testOneToOneWithSource();
    }

    public void testAdhocDeepFetchChained()
    {
        getTestAdhocDeepFetch().testChained();
    }

    public void testPartialDeepAdhocDeepFetch()
    {
        getTestAdhocDeepFetch().testPartialDeepAdhocDeepFetch();
    }

    public void testPartialDeepAdhocDeepFetchMultiAttribute()
    {
        getTestAdhocDeepFetch().testPartialDeepAdhocDeepFetchMultiAttribute();
    }

    public void testPartialDeepAdhocDeepFetchWithFromIsInclusiveTempTable()
    {
        getTestAdhocDeepFetch().testPartialDeepAdhocDeepFetchWithFromIsInclusiveTempTable();
    }

    public void testAdhocDeepFetchWithFromIsInclusiveTempTable()
    {
        getTestAdhocDeepFetch().testAdhocDeepFetchWithFromIsInclusiveTempTable();
    }

    public void testCursorIterationNonDatedOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorIterationNonDatedOperationBasedList();
    }

    public void testCursorIterationNonDatedNonOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorIterationNonDatedNonOperationBasedList();
    }

    public void testCursorIterationDatedOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorIterationDatedOperationBasedList();
    }

    public void testCursorIterationDatedNonOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorIterationDatedNonOperationBasedList();
    }

    public void testCursorIterationOperationBasedWithBreak()
            throws Exception
    {
        getTestCursor().testCursorIterationOperationBasedWithBreak();
    }

    public void testCursorIterationNonOperationBasedWithBreak()
            throws Exception
    {
        getTestCursor().testCursorIterationNonOperationBasedWithBreak();
    }

    public void testCursorIterationWithDeepFetch()
            throws Exception
    {
        getTestCursor().testCursorIterationWithDeepFetch();
    }

    public void testCursorIterationWithMultipleDesks()
            throws Exception
    {
        getTestCursor().testCursorIterationWithMultipleDesks();
    }

    public void testCursorWithLargeInClauseAndEmptyTable()
            throws Exception
    {
        getTestCursor().testCursorWithLargeInClauseAndEmptyTable();
    }

    public void testCursorInTransactionIterationNonDatedOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationNonDatedOperationBasedList();
    }

    public void testCursorInTransactionIterationNonDatedNonOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationNonDatedNonOperationBasedList();
    }

    public void testCursorInTransactionIterationDatedOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationDatedOperationBasedList();
    }

    public void testCursorInTransactionIterationDatedNonOperationBasedList()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationDatedNonOperationBasedList();
    }

    public void testCursorInTransactionIterationOperationBasedWithBreak()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationOperationBasedWithBreak();
    }

    public void testCursorInTransactionIterationNonOperationBasedWithBreak()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationNonOperationBasedWithBreak();
    }

    public void testCursorInTransactionIterationWithDeepFetch()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationWithDeepFetch();
    }

    public void testCursorInTransactionIterationWithMultipleDesks()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationWithMultipleDesks();
    }

    public void testCursorInTransactionWithLargeInClauseAndEmptyTable()
            throws Exception
    {
        getTestCursor().testCursorInTransactionWithLargeInClauseAndEmptyTable();
    }

    public void testCursorWithLargeResult()
    {
        getTestCursor().testCursorWithLargeResult();
    }

    public void testCursorWithLargeResultParallel()
    {
        getTestCursor().testCursorWithLargeResult(4);
    }

    public void testCursorInTransactionWithLargeResult()
            throws Exception
    {
        getTestCursor().testCursorInTransactionWithLargeResult();
    }

    public void testCursorWithLargeResultAndAborted()
    {
        getTestCursor().testCursorWithLargeResultAndAborted();
    }

    public void testCursorInTransactionWithLargeResultAndAborted()
            throws Exception
    {
        getTestCursor().testCursorInTransactionWithLargeResultAndAborted();
    }

    public void testCursorIterationOperationBasedWithException()
    {
        getTestCursor().testCursorIterationOperationBasedWithException();
    }

    public void testCursorInTransactionIterationOperationBasedWithException()
            throws Exception
    {
        getTestCursor().testCursorInTransactionIterationOperationBasedWithException();
    }

    public void testSmallTupleIn()
    {
        getTestTupleIn().testSmallTupleIn();
    }

    public void testSmallTupleInWithSet()
    {
        getTestTupleIn().testSmallTupleInWithSet();
    }

    public void testTupleInInTransactionWithSource()
    {
        getTestTupleIn().testTupleInInTransactionWithSource();
    }

    public void testTupleInInTransactionWithSourceTwice()
    {
        getTestTupleIn().testTupleInInTransactionWithSourceTwice();
    }

    public void testMediumTupleIn()
    {
        getTestTupleIn().testMediumTupleIn();
    }

    public void testMediumTupleInWithSet()
    {
        getTestTupleIn().testMediumTupleInWithSet();
    }

    public void testTupleInWithRelationship()
    {
        getTestTupleIn().testTupleInWithRelationship();
    }

    public void testStringWildEqNoSql()
    {
        getTestStringLike().testStringWildEqNoSql();
    }

    public void testStringWildNotEqNoSql()
    {
        getTestStringLike().testStringWildNotEqNoSql();
    }

    public void testCanCreateAggregateBeanList()
    {
        getTestAggregateBeanList().testCanCreateAggregateBeanList();
    }

    public void testAggregateBeanListCanHandleNullResultSet()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSet();
    }

    public void testAggregateBeanListCanHandleNullResultSetForInteger()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForInteger();
    }

    public void testAggregateBeanListCanHandleNullResultSetForInteger2()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForInteger2();
    }

    public void testAggregateBeanListCanHandleNullResultSetForDouble()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForDouble();
    }

    public void testAggregateBeanListCanHandleNullResultSetForBoolean()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForBoolean();
    }

    public void testAggregateBeanListCanHandleNullResultSetForCharacter()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForCharacter();
    }

    public void testAggregateBeanListCanHandleNullResultSetForShort()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForShort();
    }

    public void testAggregateBeanListMustThrowForNullByteResultSet()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullByteResultSet();
    }

    public void testAggregateBeanListCanHandleNullResultSetForFloat()
    {
        getTestAggregateBeanList().testAggregateBeanListCanHandleNullResultSetForFloat();
    }

    public void testAggregateBeanListCountWithoutGroupByForEmptyTable()
    {
        getTestAggregateBeanList().testAggregateBeanListCountWithoutGroupByForEmptyTable();
    }

    public void testAggregateBeanListAddingAttributeWithDifferentTopLevelPortal()
    {
        getTestAggregateBeanList().testAggregateBeanListAddingAttributeWithDifferentTopLevelPortal();
    }

    public void testAggregateBeanListAddingGroupByAttributeWithDifferentTopLevelPortal()
    {
        getTestAggregateBeanList().testAggregateBeanListAddingGroupByAttributeWithDifferentTopLevelPortal();
    }

    public void testAggregateBeanListAddingTwoAggregateAttributesWithTheSameName()
    {
        getTestAggregateBeanList().testAggregateBeanListAddingTwoAggregateAttributesWithTheSameName();
    }

    public void testAggregateBeanListAddingTwoAttributeWithTheSameName()
    {
        getTestAggregateBeanList().testAggregateBeanListAddingTwoAttributeWithTheSameName();
    }

    public void testAggregateBeanListGroupByDivdedBy()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByDivdedBy();
    }

    public void testAggregateBeanListGroupByTimes()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByTimes();
    }

    public void testAggregateBeanListGroupByPlus()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByPlus();
    }

    public void testAggregateBeanListSumWithLargeInClause()
    {
        getTestAggregateBeanList().testAggregateBeanListSumWithLargeInClause();
    }

    public void testAggregateBeanListGroupByDivdedByIntDouble()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByDivdedByIntDouble();
    }

    public void testAggregateBeanListGroupByPlusIntDouble()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByPlusIntDouble();
    }

    public void testAggregateBeanListGroupByTimesIntDouble()
    {
        getTestAggregateBeanList().testAggregateBeanListGroupByTimesIntDouble();
    }

    public void testAggregateBeanListCountOnRelated()
    {
        getTestAggregateBeanList().testAggregateBeanListCountOnRelated();
    }

    public void testAggregateBeanListCountOnRelatedAfterCaching()
    {
        getTestAggregateBeanList().testAggregateBeanListCountOnRelatedAfterCaching();
    }

    public void testAggregateBeanListWithLargeIn()
    {
        getTestAggregateBeanList().testAggregateBeanListWithLargeIn();
    }

//    @Override
//    public void testCanDoAggregateAndGroupByOperationsOnString()
//    {
//        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnString();
//    }

    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveInteger()
    {
        getTestAggregateBeanListWithPrimitives().testCanDoAggregateAndGroupByOperationsOnPrimitiveInteger();
    }

    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveDouble()
    {
        getTestAggregateBeanListWithPrimitives().testCanDoAggregateAndGroupByOperationsOnPrimitiveDouble();
    }

    public void testCanDoAggregateAndGroupByOperationsOnPrimitiveBoolean()
    {
        getTestAggregateBeanListWithPrimitives().testCanDoAggregateAndGroupByOperationsOnPrimitiveBoolean();
    }

    public void testCanDoAggregateAndGroupByOperationsOnInteger()
    {
        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnInteger();
    }

    public void testCanDoAggregateAndGroupByOperationsOnDouble()
    {
        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnDouble();
    }

    public void testCanDoAggregateAndGroupByOperationsOnBoolean()
    {
        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnBoolean();
    }

    public void testCanDoAggregateAndGroupByOperationsOnDate() throws ParseException
    {
        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnDate();
    }

    public void testCanDoAggregateAndGroupByOperationsOnTimestamp()
    {
        getTestAggregateBeanList().testCanDoAggregateAndGroupByOperationsOnTimestamp();
    }

    public void testSimpleAggregationOnPrimitive()
    {
        getTestAggregateBeanListWithPrimitives().testSimpleAggregationOnPrimitive();
    }

    public void testAggregateBeanListCanHandleNullResultSetForString()
    {
        getTestAggregateBeanListWithPrimitives().testAggregateBeanListCanHandleNullResultSetForString();
    }

    public void testMustThrowForNullPrimitiveIntResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveIntResultSet();
    }

    public void testMustThrowForNullPrimitiveDoubleResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveDoubleResultSet();
    }

    public void testMustThrowForNullPrimitiveBooleanResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveBooleanResultSet();
    }

    public void testMustThrowForNullPrimitiveCharResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveCharResultSet();
    }

    public void testMustThrowForNullPrimitiveShortResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveShortResultSet();
    }

    public void testMustThrowForNullPrimitiveByteResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveByteResultSet();
    }

    public void testMustThrowForNullPrimitiveFloatResultSet()
    {
        getTestAggregateBeanListWithPrimitives().testMustThrowForNullPrimitiveFloatResultSet();
    }

    public void testInvalidOrderByAttribute()
    {
        getAggregateListWithOrderBy().testInvalidOrderByAttribute();
    }

    public void testSimpleOrderBySellerName()
    {
        getAggregateListWithOrderBy().testSimpleOrderBySellerName();
    }

    public void testMultipleOrderBys()
    {
        getAggregateListWithOrderBy().testMultipleOrderBys();
    }

    public void testAscendingOrderByCountForUnresolvedList()
    {
        getAggregateListWithOrderBy().testAscendingOrderByCountForUnresolvedList();
    }

    public void testDescendingOrderByManufactureIdForResolvedList()
    {
        getAggregateListWithOrderBy().testDescendingOrderByManufactureIdForResolvedList();
    }

    public void testAggregateBeanListInvalidOrderByAttribute()
    {
        getTestAggregateBeanListOrderBy().testAggregateBeanListInvalidOrderByAttribute();
    }

    public void testAggregateBeanListSimpleOrderBySellerName()
    {
        getTestAggregateBeanListOrderBy().testAggregateBeanListSimpleOrderBySellerName();
    }

    public void testAggregateBeanListMultipleOrderBys()
    {
        getTestAggregateBeanListOrderBy().testAggregateBeanListMultipleOrderBys();
    }

    public void testAggregateBeanListAscendingOrderForUnresolvedList()
    {
        getTestAggregateBeanListOrderBy().testAggregateBeanListAscendingOrderForUnresolvedList();
    }

    public void testAggregateBeanListAscendingAndDescendingOrderForResolvedList()
    {
        getTestAggregateBeanListOrderBy().testAggregateBeanListAscendingAndDescendingOrderForResolvedList();
    }

    public void testStringHavingEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingEqForAggregateBeanList();
    }

    public void testStringHavingEqWithDifferentAttributeForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingEqWithDifferentAttributeForAggregateBeanList();
    }

    public void testStringHavingNotEqForAggregateBeanListForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingNotEqForAggregateBeanListForAggregateBeanList();
    }

    public void testStringHavingGreaterThanForAggregateBeanListForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingGreaterThanForAggregateBeanListForAggregateBeanList();
    }

    public void testStringHavingGreaterThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingGreaterThanEqForAggregateBeanList();
    }

    public void testStringHavingLessThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingLessThanForAggregateBeanList();
    }

    public void testStringHavingLessThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testStringHavingLessThanEqForAggregateBeanList();
    }

    public void testTimestampHavingEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingEqForAggregateBeanList();
    }

    public void testTimestampHavingNotEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingNotEqForAggregateBeanList();
    }

    public void testTimestampHavingGreaterThanForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingGreaterThanForAggregateBeanList();
    }

    public void testTimestampHavingGreaterThanEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingGreaterThanEqForAggregateBeanList();
    }

    public void testTimestampHavingLessThanForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingLessThanForAggregateBeanList();
    }

    public void testTimestampHavingLessThanEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testTimestampHavingLessThanEqForAggregateBeanList();
    }

    public void testDateHavingEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingEqForAggregateBeanList();
    }

    public void testDateHavingNotEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingNotEqForAggregateBeanList();
    }

    public void testDateHavingGreaterThanForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingGreaterThanForAggregateBeanList();
    }

    public void testDateHavingGreaterThanEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingGreaterThanEqForAggregateBeanList();
    }

    public void testDateHavingLessThanForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingLessThanForAggregateBeanList();
    }

    public void testDateHavingLessThanEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDateHavingLessThanEqForAggregateBeanList();
    }

    public void testBooleanHavingEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingEqForAggregateBeanList();
    }

    public void testBooleanHavingNotEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingNotEqForAggregateBeanList();
    }

    public void testBooleanHavingGreaterThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingGreaterThanForAggregateBeanList();
    }

    public void testBooleanHavingGreaterThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingGreaterThanEqForAggregateBeanList();
    }

    public void testBooleanHavingLessThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingLessThanForAggregateBeanList();
    }

    public void testBooleanHavingLessThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testBooleanHavingLessThanEqForAggregateBeanList();
    }

    public void testCharHavingEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingEqForAggregateBeanList();
    }

    public void testCharHavingNotEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingNotEqForAggregateBeanList();
    }

    public void testCharHavingGreaterThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingGreaterThanForAggregateBeanList();
    }

    public void testCharHavingGreaterThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingGreaterThanEqForAggregateBeanList();
    }

    public void testCharHavingLessThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingLessThanForAggregateBeanList();
    }

    public void testCharHavingLessThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCharHavingLessThanEqForAggregateBeanList();
    }

    public void testIntegerHavingEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingEqForAggregateBeanList();
    }

    public void testIntegerHavingNotEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingNotEqForAggregateBeanList();
    }

    public void testIntegerHavingGreaterThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingGreaterThanForAggregateBeanList();
    }

    public void testIntegerHavingGreaterThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingGreaterThanEqForAggregateBeanList();
    }

    public void testIntegerHavingLessThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingLessThanForAggregateBeanList();
    }

    public void testIntegerHavingLessThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testIntegerHavingLessThanEqForAggregateBeanList();
    }

    public void testDoubleHavingEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingEqForAggregateBeanList();
    }

    public void testDoubleHavingNotEqForAggregateBeanList() throws ParseException
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingNotEqForAggregateBeanList();
    }

    public void testDoubleHavingGreaterThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingGreaterThanForAggregateBeanList();
    }

    public void testDoubleHavingGreaterThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingGreaterThanEqForAggregateBeanList();
    }

    public void testDoubleHavingLessThanForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingLessThanForAggregateBeanList();
    }

    public void testDoubleHavingLessThanEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testDoubleHavingLessThanEqForAggregateBeanList();
    }

    public void testCountAggregationWithHavingForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testCountAggregationWithHavingForAggregateBeanList();
    }

    public void testMappedDoubleHavingEqForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testMappedDoubleHavingEqForAggregateBeanList();
    }

    public void testMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList();
    }

    public void testDatedMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testDatedMappedDoubleHavingEqUsingDifferentAttributeInHavingForAggregateBeanList();
    }

    public void testAndHavingOperationForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testAndHavingOperationForAggregateBeanList();
    }

    public void testOrHavingOperationForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testOrHavingOperationForAggregateBeanList();
    }

    public void testComplexHavingOperationForAggregateBeanList()
    {
        getTestAggregateBeanListWithHavingClause().testComplexHavingOperationForAggregateBeanList();
    }

    public void testStringHavingEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingEqForPrimitiveBeanList();
    }

    public void testStringHavingEqWithDifferentAttributeForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingEqWithDifferentAttributeForPrimitiveBeanList();
    }

    public void testStringHavingNotEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingNotEqForPrimitiveBeanList();
    }

    public void testStringHavingGreaterThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingGreaterThanForPrimitiveBeanList();
    }

    public void testStringHavingGreaterThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testStringHavingLessThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingLessThanForPrimitiveBeanList();
    }

    public void testStringHavingLessThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testStringHavingLessThanEqForPrimitiveBeanList();
    }

    public void testTimestampHavingEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingEqForPrimitiveBeanList();
    }

    public void testTimestampHavingNotEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingNotEqForPrimitiveBeanList();
    }

    public void testTimestampHavingGreaterThanForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingGreaterThanForPrimitiveBeanList();
    }

    public void testTimestampHavingGreaterThanEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testTimestampHavingLessThanForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingLessThanForPrimitiveBeanList();
    }

    public void testTimestampHavingLessThanEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testTimestampHavingLessThanEqForPrimitiveBeanList();
    }

    public void testDateHavingEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingEqForPrimitiveBeanList();
    }

    public void testDateHavingNotEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingNotEqForPrimitiveBeanList();
    }

    public void testDateHavingGreaterThanForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingGreaterThanForPrimitiveBeanList();
    }

    public void testDateHavingGreaterThanEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testDateHavingLessThanForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingLessThanForPrimitiveBeanList();
    }

    public void testDateHavingLessThanEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDateHavingLessThanEqForPrimitiveBeanList();
    }

    public void testBooleanHavingEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingEqForPrimitiveBeanList();
    }

    public void testBooleanHavingNotEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingNotEqForPrimitiveBeanList();
    }

    public void testBooleanHavingGreaterThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingGreaterThanForPrimitiveBeanList();
    }

    public void testBooleanHavingGreaterThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testBooleanHavingLessThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingLessThanForPrimitiveBeanList();
    }

    public void testBooleanHavingLessThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testBooleanHavingLessThanEqForPrimitiveBeanList();
    }

    public void testCharHavingEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingEqForPrimitiveBeanList();
    }

    public void testCharHavingNotEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingNotEqForPrimitiveBeanList();
    }

    public void testCharHavingGreaterThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingGreaterThanForPrimitiveBeanList();
    }

    public void testCharHavingGreaterThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testCharHavingLessThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingLessThanForPrimitiveBeanList();
    }

    public void testCharHavingLessThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCharHavingLessThanEqForPrimitiveBeanList();
    }

    public void testIntegerHavingEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingEqForPrimitiveBeanList();
    }

    public void testIntegerHavingNotEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingNotEqForPrimitiveBeanList();
    }

    public void testIntegerHavingGreaterThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingGreaterThanForPrimitiveBeanList();
    }

    public void testIntegerHavingGreaterThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testIntegerHavingLessThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingLessThanForPrimitiveBeanList();
    }

    public void testIntegerHavingLessThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testIntegerHavingLessThanEqForPrimitiveBeanList();
    }

    public void testDoubleHavingEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingEqForPrimitiveBeanList();
    }

    public void testDoubleHavingNotEqForPrimitiveBeanList() throws ParseException
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingNotEqForPrimitiveBeanList();
    }

    public void testDoubleHavingGreaterThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingGreaterThanForPrimitiveBeanList();
    }

    public void testDoubleHavingGreaterThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingGreaterThanEqForPrimitiveBeanList();
    }

    public void testDoubleHavingLessThanForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingLessThanForPrimitiveBeanList();
    }

    public void testDoubleHavingLessThanEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDoubleHavingLessThanEqForPrimitiveBeanList();
    }

    public void testCountAggregationWithHavingForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testCountAggregationWithHavingForPrimitiveBeanList();
    }

    public void testMappedDoubleHavingEqForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testMappedDoubleHavingEqForPrimitiveBeanList();
    }

    public void testMappedDoubleHavingEqUsingDifferentAttributeInHavingForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testMappedDoubleHavingEqUsingDifferentAttributeInHavingForPrimitiveBeanList();
    }

    public void testDatedMappedDoubleHavingEqUsingDifferentAttributeInHavingForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testDatedMappedDoubleHavingEqUsingDifferentAttributeInHavingForPrimitiveBeanList();
    }

    public void testAndHavingOperationForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testAndHavingOperationForPrimitiveBeanList();
    }

    public void testOrHavingOperationForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testOrHavingOperationForPrimitiveBeanList();
    }

    public void testComplexHavingOperationForPrimitiveBeanList()
    {
        getTestAggregatePrimitiveBeanListWithHavingClause().testComplexHavingOperationForPrimitiveBeanList();
    }

    public void testAddingAttributeToAggregateListAfterBeingResolved()
    {
        getTestAggregateBeanListImmutability().testAddingAttributeToAggregateListAfterBeingResolved();
    }

    public void testAddingElementToAggregateList()
    {
        getTestAggregateBeanListImmutability().testAddingElementToAggregateList();
    }

    public void testAddingIndexToAggregateList()
    {
        getTestAggregateBeanListImmutability().testAddingIndexToAggregateList();
    }

    public void testSettingElementToAggregateList()
    {
        getTestAggregateBeanListImmutability().testSettingElementToAggregateList();
    }

    public void testAddingCollectionToAggregateList()
    {
        getTestAggregateBeanListImmutability().testAddingCollectionToAggregateList();
    }

    public void testRemovingElementByIndexFromAggregateList()
    {
        getTestAggregateBeanListImmutability().testRemovingElementByIndexFromAggregateList();
    }

    public void testRemovingElementUsingObjectFromAggregateList()
    {
        getTestAggregateBeanListImmutability().testRemovingElementUsingObjectFromAggregateList();
    }

    public void testClearingAggregateList()
    {
        getTestAggregateBeanListImmutability().testClearingAggregateList();
    }

    public void testRetainingElementsInAggregateList()
    {
        getTestAggregateBeanListImmutability().testRetainingElementsInAggregateList();
    }

    public void testCanCreateAggregateBeanListForOnlyExtendedMethods()
    {
        getTestAggregateBeanListForSubclass().testCanCreateAggregateBeanListForOnlyExtendedMethods();
    }

    public void testCanCreateAggregateBeanListForOnlyDeclaredMethods()
    {
        getTestAggregateBeanListForSubclass().testCanCreateAggregateBeanListForOnlyDeclaredMethods();
    }

    public void testCanCreateAggregateBeanListForExtenedAndDeclaredMethods()
    {
        getTestAggregateBeanListForSubclass().testCanCreateAggregateBeanListForExtenedAndDeclaredMethods();
    }

    public void testSubclassBehaviourForNullResultSet()
    {
        getTestAggregateBeanListForSubclass().testSubclassBehaviourForNullResultSet();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeString()
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeString();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeInteger()
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeInteger();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDouble()
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDouble();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeBoolean()
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeBoolean();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDate() throws ParseException
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeDate();
    }

    public void testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeTimestamp()
    {
        getTestAggregateBeanListForSubclass().testSubclassAggregateAndGroupByOperationsOnExtendedMethodTypeTimestamp();
    }

    public void testCanCreateAggregateBeanListFromImplementedMethods()
    {
        getTestAggregateBeanListForSubclass().testCanCreateAggregateBeanListFromImplementedMethods();
    }
}
