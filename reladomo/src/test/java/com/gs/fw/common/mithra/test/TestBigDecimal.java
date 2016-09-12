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

import com.gs.collections.impl.set.mutable.primitive.DoubleHashSet;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraBusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import com.gs.collections.impl.set.mutable.UnifiedSet;



public class TestBigDecimal extends MithraTestAbstract
{
    private static final Logger logger = LoggerFactory.getLogger(TestBigDecimal.class);
    protected static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        BigOrder.class,
                        BigOrderItem.class,
                        BitemporalBigOrder.class,
                        BitemporalBigOrderItem.class,
                        MultiPkBigDecimal.class
                };
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new BigOrderResultSetComparator());
    }

    public void testBasicEqRetrieval() throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG = 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().eq(new BigDecimal("0.010")));
        this.genericRetrievalTest(sql, orders);

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG = 0.005";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().eq(new BigDecimal("0.005")));
        this.genericRetrievalTest(sql, orders);

        BigDecimal expectedDiscount = new BigDecimal("0.100");
        BigOrder order = BigOrderFinder.findOne(BigOrderFinder.discountPercentage().eq(expectedDiscount));
        BigDecimal discount = order.getDiscountPercentage();
        assertEquals(expectedDiscount, discount);
    }

    public void testEqUsingDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG = 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().eq(0.01));
        this.genericRetrievalTest(sql, orders);

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG = 0.005";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().eq(0.005));
        this.genericRetrievalTest(sql, orders);

        double expectedDiscount = 0.100;
        BigOrder order = BigOrderFinder.findOne(BigOrderFinder.discountPercentage().eq(expectedDiscount));
        BigDecimal discount = order.getDiscountPercentage();
        assertEquals(expectedDiscount, discount.doubleValue());
    }


    public void testNotEq() throws SQLException

    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG <> 0.010";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().notEq(new BigDecimal("0.010")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testNotEqWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG <> 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().notEq(0.01));

        this.genericRetrievalTest(sql, orders);
    }

    public void testLessThan()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG < 0.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThan(new BigDecimal("0.99")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testLessThanWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG < 0.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThan(0.99));

        this.genericRetrievalTest(sql, orders);
    }

    public void testGreaterThan()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG > 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThan(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testGreaterThanWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG > 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThan(0.01));

        this.genericRetrievalTest(sql, orders);
    }

    public void testLessThanEquals()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG <= 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThanEquals(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testLessThanEqualsWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG <= 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().lessThanEquals(new BigDecimal("0.01")));

        this.genericRetrievalTest(sql, orders);
    }

    public void testGreaterThanEqualsWithBigDecimal()
            throws SQLException
    {
        String sql;
        BigOrderList orders;

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG >= 0.01";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().greaterThanEquals(0.01));

        this.genericRetrievalTest(sql, orders);
    }

    public void testIn()
            throws SQLException
    {
        String sql;
        BigOrderList orders;
        Set<BigDecimal> bdSet = new UnifiedSet<BigDecimal>(3);
        bdSet.add(new BigDecimal("0.010"));
        bdSet.add(new BigDecimal("0.005"));
        bdSet.add(new BigDecimal("0.100"));
        sql = "select * from APP.BIG_ORDERS where DISC_PCTG in (0.01, 0.005, 0.100)";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().in(bdSet));
        this.genericRetrievalTest(sql, orders);
    }

    public void testInWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;
        DoubleHashSet bdSet = new DoubleHashSet(3);
        bdSet.add(0.01);
        bdSet.add(0.005);
        bdSet.add(0.100);
        sql = "select * from APP.BIG_ORDERS where DISC_PCTG in (0.01, 0.005, 0.100)";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().in(bdSet));
        this.genericRetrievalTest(sql, orders);
    }

    public void testNotIn()
            throws SQLException
    {
        String sql;
        BigOrderList orders;
        Set<BigDecimal> bdSet = new UnifiedSet<BigDecimal>(3);
        bdSet.add(new BigDecimal("0.010"));
        bdSet.add(new BigDecimal("0.005"));

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG not in (0.01, 0.005)";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().notIn(bdSet));
        this.genericRetrievalTest(sql, orders);
    }

    public void testNotInWithDouble()
            throws SQLException
    {
        String sql;
        BigOrderList orders;
        DoubleHashSet bdSet = new DoubleHashSet(3);
        bdSet.add(0.01);
        bdSet.add(0.005);

        sql = "select * from APP.BIG_ORDERS where DISC_PCTG not in (0.01, 0.005)";
        orders = BigOrderFinder.findMany(BigOrderFinder.discountPercentage().notIn(bdSet));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedEqOperation() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY = 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().eq(new BigDecimal("20.000")));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedEqWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY = 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().eq(20.000));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedNotEqWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY <> 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().notEq(20.000));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedLessThan() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY < 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().lessThan(new BigDecimal("20.000")));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedLessThanWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY < 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().lessThan(20.000));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedGreaterThan() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY > 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().greaterThan(new BigDecimal("20")));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedGreaterThanWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY > 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().greaterThan(20));
        this.genericRetrievalTest(sql, orders);
    }


    public void testMappedLessThanEquals() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY <= 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().lessThanEquals(new BigDecimal("20")));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedLessThanEqualsWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY <= 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().lessThanEquals(20));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedGreaterThanEquals() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY >= 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().greaterThanEquals(new BigDecimal("20")));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedGreaterThanEqualsWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY >= 20";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().greaterThanEquals(20));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedIn() throws SQLException
    {
        String sql;
        BigOrderList orders;
        Set<BigDecimal> set = new UnifiedSet<BigDecimal>();
        set.add(new BigDecimal("10.000"));
        set.add(new BigDecimal("20.000"));
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY in (10.000, 20.000)";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().in(set));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedInWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        DoubleHashSet set = new DoubleHashSet();
        set.add(10);
        set.add(20);
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY in (10, 20)";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().in(set));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedNotIn() throws SQLException
    {
        String sql;
        BigOrderList orders;
        Set<BigDecimal> set = new UnifiedSet<BigDecimal>();
        set.add(new BigDecimal("10.000"));
        set.add(new BigDecimal("20.000"));
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY not in (10, 20)";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().notIn(set));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedNotInWithDouble() throws SQLException
    {
        String sql;
        BigOrderList orders;
        DoubleHashSet set = new DoubleHashSet();
        set.add(10.000);
        set.add(20.000);
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.QUANTITY not in (10, 20)";
        orders = BigOrderFinder.findMany(BigOrderFinder.items().quantity().notIn(set));
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedWithParameterOperation() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.ORIGINAL_PRICE = 9.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.itemsByPrice(new BigDecimal("9.99")).exists());
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedWithDoubleParameter() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.ORIGINAL_PRICE = 9.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.itemsByDoublePrice(9.99).exists());
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedWithParameterInOperation() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 " +
                "where t0.ORDER_ID = t1.ORDER_ID " +
                "and t1.BIG_PRICE in (888888888888888888.88888888888888888888,123456789012345678.12345678901234567899) ";
        Set priceSet = new UnifiedSet<BigDecimal>(2);
        priceSet.add(new BigDecimal("888888888888888888.88888888888888888888"));
        priceSet.add(new BigDecimal("123456789012345678.12345678901234567899"));
        orders = BigOrderFinder.findMany(BigOrderFinder.itemsForProductPriceSet(priceSet).exists());
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedWithConstantOperation() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.ORIGINAL_PRICE = 9.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.nineNineNineItems().exists());
        this.genericRetrievalTest(sql, orders);
    }

    public void testMappedWithConstantOperation2() throws SQLException
    {
        String sql;
        BigOrderList orders;
        sql = "select * from APP.BIG_ORDERS t0, APP.BIG_ORDER_ITEM t1 where t0.ORDER_ID = t1.ORDER_ID and t1.ORIGINAL_PRICE <= 9.99";
        orders = BigOrderFinder.findMany(BigOrderFinder.cheapItems().exists());
        this.genericRetrievalTest(sql, orders);
    }

    public void testPrimaryKey() throws SQLException
    {

        BigOrderItemList itemList = new BigOrderItemList(BigOrderItemFinder.id().eq(new BigDecimal("9999999999")));
        assertEquals(1, itemList.size());
    }

    public void testPrimaryKeyWithDouble() throws SQLException
    {
        BigOrderItemList itemList = new BigOrderItemList(BigOrderItemFinder.id().eq(8888888888.9));
        assertEquals(1, itemList.size());
    }

    
    public void testSetInvalidBigDecimalValue()
    {
        try
        {
            BigOrder order = new BigOrder();
            order.setDiscountPercentage(new BigDecimal("999.999"));
            fail("Should not get here!");
        }
        catch (MithraBusinessException e)
        {
            logger.info("Expected excpetion: ", e);
        }

        try
        {
            //todo: moh - review this case, is the rounding correct?
            
            BigOrder order = new BigOrder();
            order.setDiscountPercentage(new BigDecimal("0.0999"));
            assertEquals(new BigDecimal("0.100"), order.getDiscountPercentage());
        }
        catch (MithraBusinessException e)
        {
            logger.info("Expected excpetion: ", e);
        }
    }

    public void testInsert()
    {
        BigDecimal discount = new BigDecimal("0.990");
        BigOrder order = this.createBigOrder(999, discount);
        order.insert();
        BigOrderFinder.clearQueryCache();
        BigOrder order2 = findOrder(999);
        assertEquals(discount, order2.getDiscountPercentage());
    }

    public void testUpdate()
    {
        final BigOrder order2 = findOrder(100);
        final BigDecimal discount = new BigDecimal("0.980");
        assertEquals(new BigDecimal("0.010"),order2.getDiscountPercentage());
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


    public void testBitemporalUpdate()
    {
        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        final int orderId = 100;
        final int itemId = 1;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
                assertNotNull(order);
                order.setDiscountPercentage(new BigDecimal("0.999"));
                return null;
            }
        });
        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
        assertNotNull(order);
        assertEquals(new BigDecimal("0.999"), order.getDiscountPercentage());

        final BigDecimal price = new BigDecimal("99999.99");
        final BigDecimal bigPrice = new BigDecimal("999999999999999999.99999999999999999999");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
                assertNotNull(item);
                item.setPrice(price);
                item.setBigPrice(bigPrice);
                return null;
            }
        });
        BitemporalBigOrderItemFinder.clearQueryCache();
        BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
        assertEquals(price, item.getPrice());
        assertEquals(bigPrice, item.getBigPrice());
    }

    public void testUpdateUntil()
            throws ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2009-01-01 12:00:00.0").getTime());
        final Timestamp untilDate = new Timestamp(timestampFormat.parse("2009-07-24 12:00:00.0").getTime());
        final int orderId = 100;
        final int itemId = 1;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
                assertNotNull(order);
                order.setDiscountPercentageUntil(new BigDecimal("0.999"), untilDate);
                return null;
            }
        });
        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
        assertNotNull(order);
        assertEquals(new BigDecimal("0.999"), order.getDiscountPercentage());

        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order2 = findDatedOrder(orderId, new Timestamp(System.currentTimeMillis()));
        assertNotNull(order2);
        assertEquals(new BigDecimal("0.010"), order2.getDiscountPercentage());

        final BigDecimal price = new BigDecimal("99999.99");
        final BigDecimal bigPrice = new BigDecimal("999999999999999999.99999999999999999999");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
                assertNotNull(item);
                item.setPriceUntil(price, untilDate);
                item.setBigPriceUntil(bigPrice, untilDate);
                return null;
            }
        });
        BitemporalBigOrderItemFinder.clearQueryCache();
        BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
        assertEquals(price, item.getPrice());
        assertEquals(bigPrice, item.getBigPrice());


        BitemporalBigOrderItem item2 = findOrderItem(itemId, new Timestamp(System.currentTimeMillis()));
        assertEquals(new BigDecimal("9.50"), item2.getPrice());
        assertEquals(new BigDecimal("999999999999999999.11111111111111111111"), item2.getBigPrice());
    }

    public void testDatedIncrement() throws ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2009-01-01 12:00:00.0").getTime());

        final int orderId = 100;
        final int itemId = 1;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
                assertNotNull(order);
                order.incrementDiscountPercentage(new BigDecimal("0.1"));
                return null;
            }
        });
        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
        assertNotNull(order);
        assertEquals(new BigDecimal("0.110"), order.getDiscountPercentage());

        final BigDecimal increment = new BigDecimal("100.99");
        final BigDecimal bigIncrement = new BigDecimal("0.11111111111111111111");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
                assertNotNull(item);
                item.incrementPrice(increment);
                item.incrementBigPrice(bigIncrement);
                return null;
            }
        });
        BitemporalBigOrderItemFinder.clearQueryCache();
        BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
        assertEquals(new BigDecimal("110.49"), item.getPrice());
        assertEquals(new BigDecimal("999999999999999999.22222222222222222222"), item.getBigPrice());
    }

    public void testIncrementUntil()
            throws SQLException, ParseException
    {
        final Timestamp businessDate = new Timestamp(timestampFormat.parse("2009-01-01 12:00:00.0").getTime());
        final Timestamp untilDate = new Timestamp(timestampFormat.parse("2009-07-24 12:00:00.0").getTime());

        final int orderId = 100;
        final int itemId = 1;

        BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
        assertNotNull(order);
        assertEquals(new BigDecimal("0.010"), order.getDiscountPercentage());

        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order2 = findDatedOrder(orderId, new Timestamp(System.currentTimeMillis()));
        assertNotNull(order2);
        assertEquals(new BigDecimal("0.010"), order2.getDiscountPercentage());


        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
                assertNotNull(order);
                BitemporalBigOrderFinder.discountPercentage().incrementUntil(order, new BigDecimal("0.1"), untilDate);
                //order.incrementDiscountPercentageUntil(new BigDecimal("0.1"), untilDate);
                return null;
            }
        });
        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order3 = findDatedOrder(orderId, businessDate);
        assertNotNull(order3);
        assertEquals(new BigDecimal("0.110"), order3.getDiscountPercentage());

        BitemporalBigOrderFinder.clearQueryCache();
        BitemporalBigOrder order4 = findDatedOrder(orderId, new Timestamp(System.currentTimeMillis()));
        assertNotNull(order4);
        assertEquals(new BigDecimal("0.010"), order4.getDiscountPercentage());


        final BigDecimal increment = new BigDecimal("100.99");
        final BigDecimal bigIncrement = new BigDecimal("0.11111111111111111111");
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
                assertNotNull(item);
                item.incrementPriceUntil(increment, untilDate);
                item.incrementBigPriceUntil(bigIncrement, untilDate);
                return null;
            }
        });
        BitemporalBigOrderItemFinder.clearQueryCache();
        BitemporalBigOrderItem item = findOrderItem(itemId, businessDate);
        assertEquals(new BigDecimal("110.49"), item.getPrice());
        assertEquals(new BigDecimal("999999999999999999.22222222222222222222"), item.getBigPrice());

        BitemporalBigOrderItem item2 = findOrderItem(itemId, new Timestamp(System.currentTimeMillis()));
        assertEquals(new BigDecimal("9.50"), item2.getPrice());
        assertEquals(new BigDecimal("999999999999999999.11111111111111111111"), item2.getBigPrice());
    }

    public void testMultiSegmentIncrementDifferentBusinesDay() throws SQLException, ParseException
    {
        BigDecimal increment = new BigDecimal("0.100");
        Timestamp priorSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-14 18:30:00").getTime());
        Timestamp businessDate = new Timestamp(timestampFormat.parse("2005-01-16 18:30:00").getTime());
        Timestamp nextSegmentBusinessDate = new Timestamp(timestampFormat.parse("2005-01-24 18:30:00").getTime());
        int orderId = 999;
        BitemporalBigOrder priorSegmentBalance = findDatedOrder(orderId, priorSegmentBusinessDate);
        BitemporalBigOrder order = findDatedOrder(orderId, businessDate);
        BitemporalBigOrder nextSegmentOrder = findDatedOrder(orderId, nextSegmentBusinessDate);
        assertNotNull(order);
        BigDecimal segmentOneOriginalDiscountPercentage = order.getDiscountPercentage();
        BigDecimal segmentTwoOriginalDiscountPercentage = nextSegmentOrder.getDiscountPercentage();
        MithraTransaction tx = MithraManagerProvider.getMithraManager().startOrContinueTransaction();
        try
        {
            findInactiveObject();
            BitemporalBigOrder fromCache = findDatedOrder(orderId, new Timestamp(businessDate.getTime() + 1000));
            //order.incrementDiscountPercentage(increment);
            BitemporalBigOrderFinder.discountPercentage().increment(order, increment);
            assertTrue(fromCache.getDiscountPercentage().equals(increment.add(segmentOneOriginalDiscountPercentage)));
            assertTrue(order.getDiscountPercentage().equals(increment.add(segmentOneOriginalDiscountPercentage)));
            assertTrue(nextSegmentOrder.getDiscountPercentage().equals(increment.add(segmentTwoOriginalDiscountPercentage)));
            assertTrue(priorSegmentBalance.getDiscountPercentage().equals(segmentOneOriginalDiscountPercentage));
            assertTrue(order.zIsParticipatingInTransaction(tx));
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            fromCache = findDatedOrder(orderId, businessDate);
            assertSame(order, fromCache);
            fromCache = findDatedOrder(orderId, new Timestamp(businessDate.getTime() + 1000));
            assertNotSame(order, fromCache);
            assertTrue(fromCache.getDiscountPercentage().equals(increment.add(segmentOneOriginalDiscountPercentage)));
            fromCache = findDatedOrder(orderId, nextSegmentBusinessDate);
            assertSame(fromCache, nextSegmentOrder);
            fromCache = findDatedOrder(orderId, priorSegmentBusinessDate);
            assertSame(fromCache, priorSegmentBalance);
            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            tx.commit();
        }
        catch (Throwable t)
        {
            getLogger().error("transaction failed", t);
            tx.rollback();
            fail("transaction failed see exception");
        }
        // check the cache:
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        BitemporalBigOrder fromCache = findDatedOrder(orderId, businessDate);
        assertSame(order, fromCache);
        assertTrue(fromCache.getDiscountPercentage().equals(increment.add(segmentOneOriginalDiscountPercentage)));
        fromCache = findDatedOrder(orderId, this.getInfinite());
        assertTrue(fromCache.getDiscountPercentage().equals(increment.add(segmentTwoOriginalDiscountPercentage)));
        fromCache = findDatedOrder(orderId, nextSegmentBusinessDate);
        assertSame(fromCache, nextSegmentOrder);
        assertTrue(nextSegmentOrder.getDiscountPercentage().equals(increment.add(segmentTwoOriginalDiscountPercentage)));
        fromCache = findDatedOrder(orderId, priorSegmentBusinessDate);
        assertSame(fromCache, priorSegmentBalance);
        assertTrue(priorSegmentBalance.getDiscountPercentage().equals(segmentOneOriginalDiscountPercentage));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

        this.checkDatedBitemporalInfinityRow(orderId, increment.add(segmentTwoOriginalDiscountPercentage), nextSegmentBusinessDate);
    }

    public Timestamp getInfinite()
    {
        return InfinityTimestamp.getParaInfinity();
    }


    private BitemporalBigOrder findDatedOrder(int orderId, Timestamp businessDate)
    {
        Operation op = BitemporalBigOrderFinder.orderId().eq(orderId);
        op = op.and(BitemporalBigOrderFinder.businessDate().eq(businessDate));

        return BitemporalBigOrderFinder.findOne(op);
    }

    private BigOrder findOrder(int orderId)
    {
        return BigOrderFinder.findOne(BigOrderFinder.orderId().eq(orderId));
    }

    protected BitemporalBigOrder findInactiveObject() throws ParseException
    {
        return BitemporalBigOrderFinder.findOne(BitemporalBigOrderFinder.orderId().eq(999)
                .and(BitemporalBigOrderFinder.businessDate().eq(new Timestamp(timestampFormat.parse("2005-01-20 00:00:00").getTime())))
                .and(BitemporalBigOrderFinder.processingDate().eq(new Timestamp(timestampFormat.parse("2004-05-05 00:00:00").getTime()))));
    }

    private BitemporalBigOrderItem findOrderItem(int itemId, Timestamp businessDate)
    {
        Operation op = BitemporalBigOrderItemFinder.id().eq(itemId);
        op = op.and(BitemporalBigOrderItemFinder.businessDate().eq(businessDate));

        return BitemporalBigOrderItemFinder.findOne(op);
    }


    public void testConstructThreeBigDecimals()
    {
        BigDecimal d = new BigDecimal("0.01");
        assertEquals(2, d.scale());
        assertEquals(1, d.precision());
        System.out.println(d);
        d = d.setScale(3);
        assertEquals(3, d.scale());
        assertEquals(2, d.precision());
        System.out.println(d);
        d = d.setScale(4);
        assertEquals(4, d.scale());
        assertEquals(3, d.precision());
        System.out.println(d);
    }

    public void testAddBigDecimals()
    {
        BigDecimal d1 = new BigDecimal("0.01").setScale(4);
        BigDecimal d2 = new BigDecimal("0.02").setScale(4);
        BigDecimal d3 = d1.add(d2);
        assertEquals(4, d3.scale());
        assertEquals(3, d3.precision());
        System.out.println(d3);
    }

    public void testForceRefreshBitemporalMultiPkList() throws SQLException
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

    public void checkDatedBitemporalTerminated(int orderId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.BITEMPORAL_BIG_ORDER where ORDER_ID = ? and " +
                " OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ps.setTimestamp(2, this.getInfinite());
        ps.setTimestamp(3, this.getInfinite());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    public void checkDatedBitemporalInfinityRow(int orderId, BigDecimal discountPercentage, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select DISC_PCTG, FROM_Z from APP.BITEMPORAL_BIG_ORDER where ORDER_ID = ? and " +
                "OUT_Z = ? and THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ps.setTimestamp(2, this.getInfinite());
        ps.setTimestamp(3, this.getInfinite());
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        BigDecimal resultQuantity = rs.getBigDecimal(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(discountPercentage.equals(resultQuantity));
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public void checkDatedBitemporalTimestampRow(int orderId, BigDecimal discountPercentage,
                                                 Timestamp businessDate, Timestamp processingDate)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select DISC_PCTG, FROM_Z from APP.BITEMPORAL_BIG_ORDER where ORDER_ID = ? and " +
                "IN_Z < ? and OUT_Z >= ? and FROM_Z <= ? and THRU_Z > ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ps.setTimestamp(2, processingDate);
        ps.setTimestamp(3, processingDate);
        ps.setTimestamp(4, businessDate);
        ps.setTimestamp(5, businessDate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        BigDecimal resultQuantity = rs.getBigDecimal(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(discountPercentage.equals(resultQuantity));
        assertFalse(hasMoreResults);
    }

    public int checkDatedBitemporalRowCounts(int orderId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from APP.BITEMPORAL_BIG_ORDER where ORDER_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, orderId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return counts;
    }

}
