
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Exchanger;

import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraUniqueIndexViolationException;
import com.gs.fw.common.mithra.TemporaryContext;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.aggregate.TestStandardDeviation;
import com.gs.fw.common.mithra.test.aggregate.TestVariance;
import com.gs.fw.common.mithra.test.domain.AllTypes;
import com.gs.fw.common.mithra.test.domain.AllTypesFinder;
import com.gs.fw.common.mithra.test.domain.AllTypesList;
import com.gs.fw.common.mithra.test.domain.BigOrder;
import com.gs.fw.common.mithra.test.domain.BigOrderFinder;
import com.gs.fw.common.mithra.test.domain.BigOrderItem;
import com.gs.fw.common.mithra.test.domain.BigOrderItemFinder;
import com.gs.fw.common.mithra.test.domain.BigOrderItemList;
import com.gs.fw.common.mithra.test.domain.BigOrderList;
import com.gs.fw.common.mithra.test.domain.MultiPkBigDecimal;
import com.gs.fw.common.mithra.test.domain.MultiPkBigDecimalList;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderDriver;
import com.gs.fw.common.mithra.test.domain.OrderDriverFinder;
import com.gs.fw.common.mithra.test.domain.OrderDriverList;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.OrderStatus;
import com.gs.fw.common.mithra.test.domain.OrderStatusList;
import com.gs.fw.common.mithra.test.domain.Product;
import com.gs.fw.common.mithra.test.domain.ProductFinder;
import com.gs.fw.common.mithra.test.domain.ProductList;
import com.gs.fw.common.mithra.test.domain.StringDatedOrder;
import com.gs.fw.common.mithra.test.domain.StringDatedOrderFinder;
import com.gs.fw.common.mithra.test.domain.StringDatedOrderList;
import com.gs.fw.common.mithra.test.domain.TestBalanceNoAcmap;
import com.gs.fw.common.mithra.test.domain.TestBalanceNoAcmapFinder;
import com.gs.fw.common.mithra.test.domain.TestBalanceNoAcmapList;
import com.gs.fw.common.mithra.test.domain.TimestampConversionFinder;
import com.gs.fw.common.mithra.test.domain.TimestampConversionList;
import com.gs.fw.common.mithra.test.domain.TimezoneTest;
import com.gs.fw.common.mithra.test.domain.TimezoneTestFinder;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.ExceptionHandlingTask;
import com.gs.fw.common.mithra.util.MithraArrayTupleTupleSet;
import com.gs.fw.common.mithra.util.TempTableNamer;
import com.gs.fw.common.mithra.util.TupleSet;
import junit.framework.Assert;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

public class TestH2GeneralTestCases
        extends MithraH2TestAbstract
{
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private TestForceRefresh testForceRefresh = new TestForceRefresh();
    private TestIn testIn = new TestIn();

    public void tearDown() throws Exception
    {
        MithraManagerProvider.getMithraManager().setTransactionTimeout(60);
        super.tearDown();
    }

    public void testStringLikeEscapes()
    {
        new TestStringLike().testStringLikeEscapes();
    }

    public void testRollback()
    {
        new CommonVendorTestCases().testRollback();
    }

    public void testTimestampGranularity() throws Exception
    {
        new CommonVendorTestCases().testTimestampGranularity();
    }

    public void testOptimisticLocking() throws Exception
    {
        new CommonVendorTestCases().testOptimisticLocking();
    }

    public void testSimpleOrder()
    {
        Assert.assertEquals(4, new ProductList(ProductFinder.all()).size());
    }

    public void testTimeTransactionalBasicTime()
    {
        new TestTimeTransactional().testBasicTime();
    }

    public void testTimeTransactionalUpdate()
    {
        new TestTimeTransactional().testUpdate();
    }

    public void testTimeTransactionalToString()
    {
        new TestTimeTransactional().testToString();
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
        new TestTimeNonTransactional().testBasicTime();
    }

    public void testTimeNonTransactionalToString()
    {
        new TestTimeNonTransactional().testToString();
    }

    public void testTimeNonTransactionalNullTime()
    {
        new TestTimeNonTransactional().testNullTime();
    }

    public void testTimeDatedTransactionalBasicTime()
    {
        new TestTimeDatedTransactional().testBasicTime();
    }

    public void testTimeDatedTransactionalUpdateTime()
    {
        new TestTimeDatedTransactional().testUpdate();
    }

    public void testTimeDatedTransactionalToString()
    {
        new TestTimeDatedTransactional().testToString();
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
        new TestTimeDatedNonTransactional().testBasicTime();
    }

    public void testTimeDatedNonTransactionalToString()
    {
        new TestTimeDatedNonTransactional().testToString();
    }

    public void testTimeDatedNonTransactionalNullTime()
    {
        new TestTimeDatedNonTransactional().testNullTime();
    }

    public void testTimeBitemporalTransactionalUpdateUntilTime()
    {
        new TestTimeBitemporalTransactional().testUpdateUntil();
    }

    public void testTimeBitemporalTransactionalInsertUntilTime()
    {
        new TestSybaseTimeTests().testBitemporalInsertUntil();
    }

    public void testTimeTuples()
    {
        new TestTimeTuple().testTupleSet();
    }

    public void testStandardDeviationDoubleInTx() throws ParseException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                new TestStandardDeviation().testStdDevDouble();
                return null;
            }
        });
    }

    public void testStandardDeviationDouble() throws ParseException
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
        new TestStandardDeviation().testStdDevBigDecimal();
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
        new TestVariance().testVarianceBigDecimal();
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
        new TestStandardDeviation().testStdDevPopBigDecimal();
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

    private String getAllTypesColumns()
    {
        return "ID,BOOL_COL,BYTE_COL,SHORT_COL,CHAR_COL,INT_COL,LONG_COL,FLOAT_COL,DOUBLE_COL,DATE_COL,TIME_COL,TIMESTAMP_COL,STRING_COL,BYTE_ARRAY_COL,NULL_BOOL_COL,NULL_BYTE_COL,NULL_SHORT_COL,NULL_CHAR_COL,NULL_INT_COL,NULL_LONG_COL,NULL_FLOAT_COL,NULL_DOUBLE_COL,NULL_DATE_COL,NULL_TIME_COL,NULL_TIMESTAMP_COL,NULL_STRING_COL,NULL_BYTE_ARRAY_COL";
    }

    public void testRetrieveOneRow()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + id;
        validateMithraResult(op, sql);
    }

    public void testRetrieveOneRowUsingTimestampInOperation()
            throws ParseException
    {
        Operation op = AllTypesFinder.timestampValue().eq(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.999").getTime()));
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where TIMESTAMP_COL = '01-JAN-07 01:01:01.999'";
        validateMithraResult(op, sql);
    }

    public void testRetrieveMultipleRows()
            throws ParseException
    {
        Operation op = AllTypesFinder.timestampValue().greaterThanEquals(new Timestamp(timestampFormat.parse("2007-01-01 01:01:01.999").getTime()));
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where TIMESTAMP_COL >= '01-JAN-07 01:01:01.999'";
        validateMithraResult(op, sql);
    }

    public void testLargeInClause()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for (int i = 0; i < setSize; i++)
        {
            idSet.add(initialId + i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + initialId;
        validateMithraResult(op, sql);
    }

    public void testLargeInClauseInParallel()
    {
        int initialId = 10000;
        int setSize = 5200;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        for (int i = 0; i < setSize; i++)
        {
            idSet.add(initialId + i);
        }

        AllTypesFinder.clearQueryCache();
        final Operation op = AllTypesFinder.id().in(idSet);
        final String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + initialId;


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
        for (int i = 0; i < setSize; i++)
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
        for (int i = 0; i < setSize; i++)
        {
            idSet.add(initialId + i);
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
        int setSize = 100;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();

        IntHashSet idSet = new IntHashSet(setSize);
        IntHashSet otherSet = new IntHashSet(setSize);
        otherSet.add(2000000000);
        for (int i = 0; i < setSize; i++)
        {
            idSet.add(initialId + i);
            otherSet.add(i);
        }

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().in(idSet).and(AllTypesFinder.intValue().in(otherSet));
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + initialId;
        validateMithraResult(op, sql);
    }

    public void testInsert()
    {
        int id = 9999999;
        AllTypes allTypesObj = this.createNewAllTypes(id, true);
        allTypesObj.insert();

        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().eq(id);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + id;
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

        long convertedOne = (long) ((doubleOne * (Math.pow(10, 2)) + 0.5));
        long convertedTwo = Math.round(doubleTwo * (Math.pow(10, 2)));

        allTypesObj = AllTypesFinder.findOneBypassCache(AllTypesFinder.id().eq(id));
    }

    //todo: fix SYBIMAGE
    public void xtestBulkInsertFloatsAndDoublesWithRoundingIssues()
    {
        int id = 10000;
        AllTypesList list = new AllTypesList();
        for (int i = 0; i < 1000; i++)
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
        String weirdChar = new String(new char[]{'a', (char) 196});
        for (int i = 0; i < 1000; i++)
        {
            Order order = new Order();
            order.setOrderId(id + i);
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
        for (int i = 0; i < 1000; i++)
        {
            BigOrder order = new BigOrder();
            order.setOrderId(id + i);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setDescription("foo");
            order.setUserId(1);
            order.setDiscountPercentage(new BigDecimal(0.123 + (i / 10000.0)));
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
        for (int i = 0; i < 1000; i++)
        {
            int mod = i % 2;


            BigOrderItem item = new BigOrderItem();
            item.setId(BigDecimal.valueOf(id + i));
            item.setOrderId(1);
            item.setProductId(1);
            if (mod == 0)
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
        order.setDescription("Order" + orderId);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setUserId(1);
        order.setDiscountPercentage(discount);
        return order;
    }

    public void testForceRefreshBitemporalMultiPkWithBigDecimalList() throws SQLException
    {
        int countToInsert = 2034;
        MultiPkBigDecimalList list = new MultiPkBigDecimalList();
        for (int i = 0; i < countToInsert; i++)
        {
            MultiPkBigDecimal testBal = new MultiPkBigDecimal();
            testBal.setId(777 + i);
            testBal.setBdId(BigDecimal.valueOf(10 + i));
            testBal.setQuantity(i * 10);
            list.add(testBal);
        }
        list.insertAll();

        MultiPkBigDecimalList list2 = new MultiPkBigDecimalList();
        list2.addAll(list);
        int count = this.getRetrievalCount();
        list2.forceRefresh();
        assertEquals(count + 1, this.getRetrievalCount());
    }

    public void testBatchInsert()
    {
        int initialId = 10000;
        int setSize = 10;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();
        AllTypesFinder.clearQueryCache();
        Operation op = AllTypesFinder.id().greaterThanEquals(initialId);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + initialId;
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

    private void timeLargeIn(int size, String type)
    {
        IntHashSet set = new IntHashSet(size);
        for (int i = 0; i < size; i++)
        {
            set.add(i + 10000);
        }
        Operation op = ProductFinder.productId().in(set);
        timeOperation(op, type + " with " + size + " params");
    }

    private void timeOperation(Operation op, String msg)
    {
        for (int i = 0; i < 5; i++)
        {
            ProductFinder.clearQueryCache();
            ProductList list = new ProductList(op);
            long now = System.currentTimeMillis();
            list.forceResolve();
            System.out.println(msg + ": took " + (System.currentTimeMillis() - now) + " ms");
        }
    }

    public void testSingleUpdate()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);
        allTypes.setIntValue(100);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + 9876;
        validateMithraResult(op, sql);
    }

    public void testMultipleUpdatesToSameObjectWithNoTransaction()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);
        allTypes.setIntValue(100);
        allTypes.setNullableBooleanValue(true);
        allTypes.setNullableByteValue((byte) 10);
        allTypes.setNullableCharValue('a');
        allTypes.setNullableShortValue((short) 1000);
        allTypes.setNullableIntValue(987654);
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + 9876;
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
                        allTypes.setNullableByteValue((byte) 10);
                        allTypes.setNullableCharValue('a');
                        allTypes.setNullableShortValue((short) 1000);
                        allTypes.setNullableIntValue(987654);
                        return null;
                    }
                }
        );
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + 9876;
        validateMithraResult(op, sql);
    }


    public void testBatchUpdate()
    {
        int initialId = 10000;
        int setSize = 1000;
        AllTypesList allTypesList = this.createNewAllTypesList(initialId, setSize);
        allTypesList.insertAll();
        final Operation op = AllTypesFinder.id().greaterThanEquals(9876);

        for (int k = 0; k < 1; k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                    new TransactionalCommand()
                    {
                        public Object executeTransaction(MithraTransaction tx) throws Throwable
                        {
                            AllTypesList allTypesList = new AllTypesList(op);
                            for (int i = 0; i < allTypesList.size(); i++)
                            {
                                AllTypes allTypes = allTypesList.get(i);
                                allTypes.setIntValue(i + count);
                            }
                            return null;
                        }
                    }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + 9876;
        validateMithraResult(op, sql);
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

        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where BOOL_COL = false";
        validateMithraResult(AllTypesFinder.booleanValue().eq(false), sql);
    }

    public void testDelete()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().eq(id);
        AllTypes allTypes = AllTypesFinder.findOne(op);

        allTypes.delete();

        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID = " + id;
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
                        for (int i = 0; i < allTypesList.size(); i++)
                        {
                            AllTypes allTypes = allTypesList.get(i);
                            allTypes.delete();
                        }

                        return null;
                    }
                }
        );

        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where BOOL_COL = true";
        validateMithraResult(AllTypesFinder.booleanValue().eq(true), sql, 0);

    }

    public void testDeleteUsingOperation()
    {
        int id = 9876;
        Operation op = AllTypesFinder.id().greaterThanEquals(id);
        AllTypesList list = new AllTypesList(op);
        list.deleteAll();
        String sql = "select " + getAllTypesColumns() + " from public.ALL_TYPES where ID >= " + id;
        validateMithraResult(op, sql, 0);
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
        Operation eq = AllTypesFinder.dateValue().year().eq(2007);
        AllTypesList one = AllTypesFinder.findMany(eq);
        int size = one.size();
        assertEquals(10, size);

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

    public void testYearTimestampRetrieval()
    {
        Operation eq = AllTypesFinder.timestampValue().year().eq(2007);
        AllTypesList one = AllTypesFinder.findMany(eq);
        int size = one.size();
        assertEquals(10, size);

        IntHashSet intSet = new IntHashSet();
        intSet.add(2005);
        intSet.add(2007);
        Operation eq2 = TimestampConversionFinder.timestampValueNone().year().in(intSet);
        TimestampConversionList one2 = TimestampConversionFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(4, size2);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.add(2005);
        intSet2.add(2007);
        Operation eq3 = TimestampConversionFinder.timestampValueUTC().year().in(intSet2);
        TimestampConversionList one4 = TimestampConversionFinder.findMany(eq3);
        int size4 = one4.size();
        assertEquals(2, size4);

        IntHashSet intSet3 = new IntHashSet();
        intSet3.add(2005);
        intSet3.add(2007);
        Operation eq4 = TimestampConversionFinder.timestampValueDB().year().in(intSet3);
        TimestampConversionList one5 = TimestampConversionFinder.findMany(eq4);
        int size5 = one5.size();
        assertEquals(3, size5);
    }

    public void testMonthTimestampRetrieval()
    {
        Operation eq = AllTypesFinder.timestampValue().month().eq(1);
        AllTypesList one = AllTypesFinder.findMany(eq);
        int size = one.size();
        assertEquals(10, size);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.add(1);
        intSet2.add(2);
        Operation eq3 = TimestampConversionFinder.timestampValueNone().month().in(intSet2);
        TimestampConversionList one3 = TimestampConversionFinder.findMany(eq3);
        int size3 = one3.size();
        assertEquals(4, size3);

        IntHashSet intSet3 = new IntHashSet();
        intSet3.add(1);
        intSet3.add(2);
        Operation eq4 = TimestampConversionFinder.timestampValueUTC().month().in(intSet3);
        TimestampConversionList one4 = TimestampConversionFinder.findMany(eq4);
        int size4 = one4.size();
        assertEquals(2, size4);

        IntHashSet intSet4 = new IntHashSet();
        intSet4.add(1);
        intSet4.add(2);
        Operation eq5 = TimestampConversionFinder.timestampValueDB().month().in(intSet4);
        TimestampConversionList one5 = TimestampConversionFinder.findMany(eq5);
        int size5 = one5.size();
        assertEquals(3, size5);
    }

    public void testDayOfMonthTimestampRetrieval()
    {
        AllTypesList list = AllTypesFinder.findMany(AllTypesFinder.timestampValue().dayOfMonth().eq(1));
        assertEquals(1, list.size());

        IntHashSet intSet = new IntHashSet();
        intSet.addAll(1, 2, 3, 4);
        Operation eq2 = TimestampConversionFinder.timestampValueNone().dayOfMonth().in(intSet);
        TimestampConversionList one2 = TimestampConversionFinder.findMany(eq2);
        int size2 = one2.size();
        assertEquals(4, size2);

        IntHashSet intSet2 = new IntHashSet();
        intSet2.addAll(1, 2, 3, 4);
        Operation eq3 = TimestampConversionFinder.timestampValueUTC().dayOfMonth().in(intSet2);
        TimestampConversionList one3 = TimestampConversionFinder.findMany(eq3);
        int size3 = one3.size();
        assertEquals(2, size3);

        IntHashSet intSet3 = new IntHashSet();
        intSet3.addAll(1, 2, 3, 4);
        Operation eq4 = TimestampConversionFinder.timestampValueDB().dayOfMonth().in(intSet3);
        TimestampConversionList one4 = TimestampConversionFinder.findMany(eq4);
        int size4 = one4.size();
        assertEquals(3, size4);
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
        for (int i = 0; i < 1000000; i++)
        {
            assertFalse(first.equals(TempTableNamer.getNextTempTableName()));
        }
        UnifiedSet set = new UnifiedSet(10000);
        for (int i = 0; i < 10000; i++)
        {
            String s = TempTableNamer.getNextTempTableName();
            assertFalse(set.contains(s));
            set.add(s);
        }
    }

    public void testDeadLockDetectionWithFoundObjects()
    {
        for (int i = 0; i < 1; i++)
        {
            this.innerTestDeadlockDetectionWithFoundObjects();
        }
    }

    private OrderStatusList createNewOrderStatusList(int initialId, int size)
    {
        OrderStatusList list = new OrderStatusList();
        for (int i = 0; i < size; i++)
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
        Runnable orderCancellingThread = new Runnable()
        {
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
        Runnable orderFillingThread = new Runnable()
        {
            public void run()
            {
                final OrderItem item = OrderItemFinder.findOne(OrderItemFinder.orderId().eq(1));
                final Order order = item.getOrder();
                waitForOtherThread(rendezvous);
                waitForOtherThread(rendezvous);
                final int[] tryValue = new int[1];
                MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand()
                {
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
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand()
        {
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
        for (int i = 1; i < 300; i++)
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
        for (int i = 0; i < 1000; i++)
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
        MithraManagerProvider.getMithraManager().setTransactionTimeout(30);
        OrderList firstList = new OrderList(OrderFinder.userId().eq(999));
        firstList.deleteAllInBatches(75000);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void testDeleteAllInBatchesWithIn()
    {
        OrderList list = createOrderList(5000, 1000);
        list.bulkInsertAll();

        IntHashSet ids = new IntHashSet(5000);
        for (int i = 1000; i < (6000); i++)
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
        list.deleteAllInBatches(15000);
        validateMithraResult(OrderFinder.userId().eq(999), "SELECT * FROM ORDERS WHERE USER_ID = 999", 0);
    }

    public void xtestInsertWithDateAsString()
            throws Exception
    {
        StringDatedOrder order = new StringDatedOrder();
        int orderId = 9876;
        Timestamp ts = new Timestamp(timestampFormat.parse("2008-03-29 18:30:00.0").getTime());
        Date dt = dateFormat.parse("2008-03-29");
        order.setOrderId(orderId);
        order.setDescription("Order " + orderId);
        order.setUserId(1);
        order.setOrderDate(dt);
        order.setProcessingDate(ts);
        order.insert();
        StringDatedOrderFinder.clearQueryCache();

        StringDatedOrder order2 = StringDatedOrderFinder.findOne(StringDatedOrderFinder.orderDate().eq(dt).and(StringDatedOrderFinder.processingDate().eq(ts)));
        assertNotNull(order2);
    }

    public void xtestDeleteObjectWithDateAsString()
    {
        Operation op = StringDatedOrderFinder.orderId().eq(1);
        op = op.and(StringDatedOrderFinder.processingDate().eq(Timestamp.valueOf("2004-01-12 00:00:00.0")));

        StringDatedOrder order = StringDatedOrderFinder.findOne(op);
        assertNotNull(order);
        order.delete();
        StringDatedOrder order2 = StringDatedOrderFinder.findOne(op);
        assertNull(order2);
    }

    public void xtestBatchDeleteWithDateAsString()
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

    public void xtestUpdateObjectWithSqlDateAsString()
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

    public void xtestUpdateObjectWithUtilDateAsString2()
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
        for (int i = 0; i < count; i++)
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
        ExceptionHandlingTask[] read = new ExceptionHandlingTask[readThreads];
        for (int i = 0; i < readThreads; i++)
        {
            read[i] = new ReadThread(rand);
            new Thread(read[i]).start();
        }

        ExceptionHandlingTask[] write = new ExceptionHandlingTask[writeThreads];
        for (int i = 0; i < writeThreads; i++)
        {
            write[i] = new WriteThread(rand);
            new Thread(write[i]).start();
        }

        for (ExceptionHandlingTask t : read) t.waitUntilDoneWithExceptionHandling();
        for (ExceptionHandlingTask t : write) t.waitUntilDoneWithExceptionHandling();
    }

    private class ReadThread extends ExceptionHandlingTask
    {
        private Random rand;

        private ReadThread(Random rand)
        {
            this.rand = rand;
        }

        public void execute()
        {
            for (int i = 0; i < 100; i++)
            {
                final int start = 1000 + rand.nextInt(500) * 2;
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        IntHashSet set = new IntHashSet(10);
                        for (int j = 0; j < 10; j++)
                        {
                            set.add(start + j);
                        }

                        OrderList list = new OrderList(OrderFinder.orderId().in(set));
                        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                        assertEquals(10, list.size());

                        for (int i = 0; i < list.size(); i += 2)
                        {
                            assertEquals(list.get(i).getUserId(), list.get(i + 1).getUserId());
                        }
                        return null;
                    }
                });
            }
        }
    }

    private class WriteThread extends ExceptionHandlingTask
    {
        private Random rand;

        private WriteThread(Random rand)
        {
            this.rand = rand;
        }

        public void execute()
        {
            for (int i = 0; i < 100; i++)
            {
                final int start = 1000 + rand.nextInt(500) * 2;
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        IntHashSet set = new IntHashSet(2);
                        for (int j = 0; j < 2; j++)
                        {
                            set.add(start + j);
                        }

                        OrderList list = new OrderList(OrderFinder.orderId().in(set));
                        list.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
                        assertEquals(2, list.size());

                        list.get(0).setUserId(list.get(0).getUserId() + 1);
                        list.get(1).setUserId(list.get(1).getUserId() + 1);

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

    public void testTupleIn()
    {
        TupleSet set = new MithraArrayTupleTupleSet();
        set.add("AA", "Product 1", 1, 100.10f);
        set.add("AA", "Product 2", 1, 120.20f);
        set.add("AB", "Product 3", 2, 300.0f);
        set.add("AB", "Product 4", 3, 400.0f);

        ProductList list = new ProductList(ProductFinder.productCode().tupleWith(ProductFinder.productDescription(), ProductFinder.manufacturerId(), ProductFinder.dailyProductionRate()).in(set));
        assertEquals(4, list.size());
    }

    public void testUniqueIndexViolationForH2Insert()
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
        catch (MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
        }
        catch (RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForH2MultiInsert()
    {
        try
        {
            final ProductList productList = new ProductList();

            // default batch size is 32, so create product objects with 2 batches which should all get inserted successfully
            for (int i = 5; i < 69; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            // now add elements which violate unique index
            for (int i = 1; i < 33; i++)
            {
                Product product = new Product();
                product.setProductId(i);
                productList.add(product);
            }

            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    productList.insertAll();
                    return null;
                }
            });
        }
        catch (MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 1"));
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 31"));
        }
        catch (RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
    }

    public void testUniqueIndexViolationForH2Update() throws SQLException
    {
        Connection con = H2TestConnectionManager.getInstance().getConnection();
        String dropSql = "drop index IF EXISTS PROD_DESC";
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
        catch (MithraUniqueIndexViolationException e)
        {
            assertTrue("Exception doesn't contain the expected primary key", e.getMessage().contains("Primary Key: productId: 2"));
        }
        catch (RuntimeException e)
        {
            fail("Should have thrown a duplicate insert exception with Primary Key");
        }
        finally
        {
            if (con == null)
            {
                con = H2TestConnectionManager.getInstance().getConnection();
            }
            con.createStatement().execute(dropSql);
            con.close();
        }
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
