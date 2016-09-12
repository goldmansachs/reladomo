

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
import com.gs.collections.impl.set.mutable.primitive.*;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.AtomicEqualityOperation;
import com.gs.fw.common.mithra.finder.InOperation;
import com.gs.fw.common.mithra.finder.None;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.util.DoWhileProcedure;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestIn
extends TestSqlDatatypes
{

    public void testBasicInRetrieval()
    throws SQLException
    {
        String sql;
        List desks;

        //Boolean
        BooleanHashSet boolSet = new BooleanHashSet();
        boolSet.add(true);
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN in ( 1 ) ";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().in(boolSet));
        this.genericRetrievalTest(sql, desks);
        assertTrue(desks.size() > 0);

        boolSet.clear();
        boolSet.add(false);
        sql = "select * from PARA_DESK where ACTIVE_BOOLEAN in ( 0 ) ";
        desks = new ParaDeskList(ParaDeskFinder.activeBoolean().in(boolSet));
        this.genericRetrievalTest(sql, desks);
        assertTrue(desks.size() > 0);

//        //Byte
//        ByteSet byteSet = new ByteHashSet();
//        byteSet.add((byte)100);
//        byteSet.add((byte)200);
//        byteSet.add((byte)300);
//        byteSet.add((byte)400);
//        byteSet.add((byte)500);
//        byteSet.add((byte)600);
//        byteSet.add((byte)700);
//        byteSet.add((byte)800);
//        byteSet.add((byte)900);
//        sql = "select * from PARA_DESK where LOCATION_BYTE in (100, 200, 300, 400, 500, 600, 700, 800, 900)";
//        desks = new ParaDeskList(ParaDeskFinder.locationByte().in(byteSet));
//        this.genericRetrievalTest(sql, desks);

        //Byte
        ByteHashSet byteSet = new ByteHashSet();
        byteSet.add((byte) 10);
        byteSet.add((byte) 20);
        byteSet.add((byte) 30);
        byteSet.add((byte) 40);
        byteSet.add((byte) 50);
        byteSet.add((byte) 127);
        byteSet.add((byte) -127);
        byteSet.add((byte) -128);
        byteSet.add((byte) 127);
        sql = "select * from PARA_DESK where LOCATION_BYTE in (10, 20, 30, 40, 50, 127, -127, -128, 127)";
        desks = new ParaDeskList(ParaDeskFinder.locationByte().in(byteSet));
        this.genericRetrievalTest(sql, desks);

        //Char
        CharHashSet charSet = new CharHashSet();
        charSet.clear();
        charSet.add('O');
        charSet.add('P');
        charSet.add('G');
        charSet.add('T');
        sql = "select * from PARA_DESK where STATUS_CHAR in ( 'O', 'P', 'G', 'T' )";
        desks = new ParaDeskList(ParaDeskFinder.statusChar().in(charSet));
        this.genericRetrievalTest(sql, desks);

        //Date
        HashSet objectSet = new HashSet();
        objectSet.add(getDawnOfTime());
        objectSet.add(getTestDate());
        sql = "select * from PARA_DESK where CLOSED_DATE = '1900-01-01' or CLOSED_DATE = '1981-06-08' ";
        desks = new ParaDeskList(ParaDeskFinder.closedDate().in(objectSet));
        this.genericRetrievalTest(sql, desks);

        //Double
        Connection connection = this.getConnection();
        DoubleHashSet doubleSet = new DoubleHashSet();
        doubleSet.add(4000000000.0);
        doubleSet.add(677673542.3);
        sql = "select * from PARA_DESK where SIZE_DOUBLE in ( ?, ? ) ";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setDouble(1, 677673542.3);
        ps.setDouble(2, 4000000000.0);
        desks = new ParaDeskList(ParaDeskFinder.sizeDouble().in(doubleSet));
        this.genericRetrievalTest(ps, desks, connection);

        //Float
        connection = this.getConnection();
        FloatHashSet floatSet = new FloatHashSet();
        floatSet.add((float) 4000000000.0);
        floatSet.add((float) 677673542.3);
        sql = "select * from PARA_DESK where MAX_FLOAT in ( ?, ? ) ";
        ps = connection.prepareStatement(sql);
        ps.setFloat(1, (float) 677673542.3);
        ps.setFloat(2, (float) 4000000000.0);
        desks = new ParaDeskList(ParaDeskFinder.maxFloat().in(floatSet));
        this.genericRetrievalTest(ps, desks, connection);

        //Integer
        IntHashSet IntHashSet = new IntHashSet();
        IntHashSet.add(100);
        IntHashSet.add(200);
        IntHashSet.add(300);
        IntHashSet.add(400);
        IntHashSet.add(500);
        IntHashSet.add(600);
        IntHashSet.add(700);
        IntHashSet.add(800);
        IntHashSet.add(900);
        sql = "select * from PARA_DESK where TAG_INT in (100, 200, 300, 400, 500, 600, 700, 800, 900)";
        desks = new ParaDeskList(ParaDeskFinder.tagInt().in(IntHashSet));
        this.genericRetrievalTest(sql, desks);

        //Long
        LongHashSet longSet = new LongHashSet();
        longSet.add(1000000);
        longSet.add(2000000);
        sql = "select * from PARA_DESK where CONNECTION_LONG in (1000000, 2000000)";
        desks = new ParaDeskList(ParaDeskFinder.connectionLong().in(longSet));
        this.genericRetrievalTest(sql, desks);

        //Short
        ShortHashSet shortSet = new ShortHashSet();
        shortSet.add((short) 1000);
        shortSet.add((short) 2000);
        sql = "select * from PARA_DESK where MIN_SHORT in (1000, 2000)";
        desks = new ParaDeskList(ParaDeskFinder.minShort().in(shortSet));
        this.genericRetrievalTest(sql, desks);

        //String
        objectSet.clear();
        objectSet.add("rnd");
        objectSet.add("cap");
        objectSet.add("lsd");
        objectSet.add("zzz");
        sql = "select * from PARA_DESK where DESK_ID_STRING in ('rnd', 'cap', 'lsd', 'zzz')";
        desks = new ParaDeskList(ParaDeskFinder.deskIdString().in(objectSet));
        this.genericRetrievalTest(sql, desks);

        //Timestamp
        objectSet.clear();
        objectSet.add(new Timestamp(getDawnOfTime().getTime()));
        objectSet.add(getTestTimestamp());
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP = '1900-01-01 00:00:00.0' or CREATE_TIMESTAMP = '1981-06-08 02:01:00.0' ";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().in(objectSet));
        this.genericRetrievalTest(sql, desks);

        objectSet.clear();
        objectSet.add(InfinityTimestamp.getParaInfinity());
        objectSet.add(getTestTimestamp());
        sql = "select * from PARA_DESK where CREATE_TIMESTAMP = '9999-12-01 23:59:00.0' or CREATE_TIMESTAMP = '1981-06-08 02:01:00.0' ";
        desks = new ParaDeskList(ParaDeskFinder.createTimestamp().in(objectSet));
        this.genericRetrievalTest(sql, desks);
    }

    public void testDoubleHugeInClause()
    {
        IntHashSet IntHashSet = new IntHashSet();
        for (int i=0;i<3000;i++)
        {
            IntHashSet.add(i);
        }
        LongHashSet longSet = new LongHashSet();
        for(int i=0;i<3000;i++)
        {
            longSet.add(i);
        }
        new ParaDeskList(ParaDeskFinder.tagInt().in(IntHashSet).and(
                ParaDeskFinder.connectionLong().in(longSet)).and(
                ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())))).forceResolve();
        // the test here is that the SQL generated is correct. Will die with exception if not.
    }

    public void testHugeInClause()
    {
        IntHashSet IntHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            IntHashSet.add(i);
        }
        new ParaDeskList(ParaDeskFinder.tagInt().in(IntHashSet).and(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())))).forceResolve();
        // the test here is that the SQL generated is correct. Will die with exception if not.
    }

    public void testDeferredPopulationOfQuestionMarksUsingMixedAndOr()
    {
        IntHashSet orderIdSet = new IntHashSet();
        for (int i = 0; i < 12; i++)
        {
            // This set must contain more than 10 entries in total and more than orderDateSet
            orderIdSet.add(3 + i);
        }
        orderIdSet.add(55);
        orderIdSet.add(56);
        orderIdSet.add(57);

        Set<Timestamp> orderDateSet = new UnifiedSet<Timestamp>();
        orderDateSet.add(Timestamp.valueOf("2004-01-12 00:00:00.0"));
        orderDateSet.add(Timestamp.valueOf("2004-04-12 00:00:00.0"));
        for (int i = 1; i <= 9; i++)
        {
            // Add some other entries just to ensure this set has more than 10 entries in total but fewer than orderIdSet
            String dayDigits = Integer.toString(i);
            if (dayDigits.length() < 2)
            {
                dayDigits = "0" + dayDigits;
            }
            orderDateSet.add(Timestamp.valueOf("2003-01-" + dayDigits + " 00:00:00.0"));
        }

        Set<String> trackingIdSet = new UnifiedSet<String>();
        trackingIdSet.add("125");
        trackingIdSet.add("126");
        trackingIdSet.add("127");
        for (int i = 0; i < 1000; i++)
        {
            // This set must be large enough to push the total size of all sets over the max search arguments threshold.
            // Additionally if we make this set larger than 4 x max search args (960 in H2) we can guarantee it will be converted to a temp table.
            // This makes it easier to predict what happens next as it allows the remaining set-based operations to be implemented as in-clauses without splitting.
            trackingIdSet.add(Integer.toString(1000 + i));
        }

        // Crucially, because the total size of all sets exceeds the max search args threshold, this means that the in-clauses are left empty during
        // initial SQL generation. The question marks are inserted only at the very end (after a decision has been taken on using temp tables or splitting).

        // This exposes an interesting edge case whereby inserting the question marks for one in-clause can shift the string position at which the other in-clause(s)
        // must be inserted. This edge case is dependent on the order of the set-based operations relative to their size.

        // A further sub-edge-case is created by mixing the use of AndOperation and OrOperation.

        // To construct this scenario our operation must meet two conditions:
        // 1) One of the in-clauses must be in an AndOperation and the other must be in an OrOperation. (OrOperation defines a mapper container; AndOperation doesn't)
        // 2) The bigger of the two in-clauses must come first (the placement of the temp table does not matter). This is why we put orderDate before orderId.
        // 3) We have to work around the fact that orderId() is indexed and so whichever sub-operation in the AndOperation it belongs to will always be moved to the front
        Operation op = OrderFinder.orderId().in(orderIdSet)
                .and(
                        OrderFinder.orderDate().in(orderDateSet)
                                .or(OrderFinder.trackingId().in(trackingIdSet))
                );

        OrderList listFromDatabase = OrderFinder.findManyBypassCache(op);
        listFromDatabase.forceResolve();
        assertEquals(5, listFromDatabase.size());

        OrderList listFromCache = OrderFinder.findMany(op);
        listFromCache.forceResolve();
        assertEquals(5, listFromCache.size());
    }

    public void testDeferredPopulationOfQuestionMarksUsingMixedOrAnd()
    {
        // Repeat the same test as above but in reverse with the first in-clause in an OrOperation and the second one in an AndOperation. This exercises a different logic path.

        IntHashSet orderIdSet = new IntHashSet();
        for (int i = 0; i < 12; i++)
        {
            // This set must contain more than 10 entries in total but unlike the test above must have more entries than orderDateSet
            orderIdSet.add(50 + i);
        }

        Set<Timestamp> orderDateSet = new UnifiedSet<Timestamp>();
        orderDateSet.add(Timestamp.valueOf("2004-01-12 00:00:00.0"));
        orderDateSet.add(Timestamp.valueOf("2004-03-12 00:00:00.0"));
        orderDateSet.add(Timestamp.valueOf("2004-04-12 00:00:00.0"));
        for (int i = 1; i <= 8; i++)
        {
            // Add some other entries to give this set 11 entries in total as it must have more than 10 entries but fewer than orderIdSet
            String dayDigits = Integer.toString(i);
            if (dayDigits.length() < 2)
            {
                dayDigits = "0" + dayDigits;
            }
            orderDateSet.add(Timestamp.valueOf("2003-01-" + dayDigits + " 00:00:00.0"));
        }

        Set<String> trackingIdSet = new UnifiedSet<String>();
        trackingIdSet.add("125");
        trackingIdSet.add("126");
        trackingIdSet.add("127");
        for (int i = 0; i < 1000; i++)
        {
            // This set must be large enough to push the total size of all sets over the max search arguments threshold.
            // Additionally if we make this set larger than 4 x max search args (960 in H2) we can guarantee it will be converted to a temp table.
            // This makes it easier to predict what happens next as it allows the remaining set-based operations to be implemented as in-clauses without splitting.
            trackingIdSet.add(Integer.toString(1000 + i));
        }

        // Notes:
        // 1) orderId in-clause must be larger than orderDate in-clause
        // 2) we had to use orderId as the first in-clause because orderId is indexed so it always comes first when the AndOperation sorts itself
        // This test additionally covers the case in which a temp table is used and the "select .... from ...." gets inserted into the empty in-clause. This, too, causes other in-clause insert positions to be shifted right.
        Operation op =
                OrderFinder.orderId().in(orderIdSet)
                        .or(OrderFinder.trackingId().in(trackingIdSet))
                .and(OrderFinder.orderDate().in(orderDateSet));

        OrderList listFromDatabase = OrderFinder.findManyBypassCache(op);
        listFromDatabase.forceResolve();
        assertEquals(5, listFromDatabase.size());

        OrderList listFromCache = OrderFinder.findMany(op);
        listFromCache.forceResolve();
        assertEquals(5, listFromCache.size());
    }

    public void testHugeInClauseWithNoBetween()
    {
        IntHashSet IntHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            IntHashSet.add(i);
        }
        InOperation inOperation = (InOperation) ParaDeskFinder.tagInt().in(IntHashSet);
        inOperation.setUseBetweenClause(false);
        new ParaDeskList(inOperation.and(ParaDeskFinder.createTimestamp().eq(new Timestamp(getDawnOfTime().getTime())))).forceResolve();
        // the test here is that the SQL generated is correct. Will die with exception if not.
    }

    public void testLargeInWithSingleQuery()
    {
        OrderList list = new OrderList();
        for(int i=0;i<2000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription(""+i);
            list.add(order);
        }
        list.insertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<1488;i+=2)
        {
            set.add(i+10000);
        }
        OrderFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.orderId().in(set));
        assertEquals(1488/2, toFind.size());
    }

    public void testLargeInWithMultipleQueries()
    {
        OrderList list = new OrderList();
        for(int i=0;i<10000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription(""+i);
            list.add(order);
        }
        list.insertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            set.add(i+10000);
        }
        OrderFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.orderId().in(set));
        assertEquals(8888/2, toFind.size());
    }

    public void testTwoLargeInWithOr()
    {
        OrderList list = new OrderList();
        for(int i=0;i<10000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription("" + i);
            order.setUserId(5000 + i);
            list.add(order);
        }
        list.insertAll();
        IntHashSet idSet = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            idSet.add(i + 10000);
        }
        IntHashSet userSet = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            userSet.add(i + 5000);
        }
        OrderFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.orderId().in(idSet).or(OrderFinder.userId().in(userSet)));
        assertEquals(8888/2, toFind.size());
    }

    public void testLargeInWithMultipleQueriesInParallel()
    {
        OrderList list = new OrderList();
        for(int i=0;i<10000;i++)
        {
            Order order = new Order();
            order.setOrderId(i + 10000);
            order.setDescription("" + i);
            list.add(order);
        }
        list.insertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            set.add(i+10000);
        }
        OrderFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.orderId().in(set));
        toFind.setNumberOfParallelThreads(5);
        assertEquals(8888 / 2, toFind.size());
    }

    public void testLargeInWithOrDeepFetch()
    {
        OrderList list = new OrderList();
        for(int i=0;i<2000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription(""+i);
            list.add(order);
        }
        list.bulkInsertAll();
        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(i+15000);
            item.setOrderId((i % 2000) + 10000);
            item.setQuantity(i);
            itemList.add(item);
        }
        itemList.bulkInsertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<20000;i++)
        {
            set.add(i+15000);
        }
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        OrderItemList toFind = OrderItemFinder.findMany(OrderItemFinder.id().in(set).and(OrderItemFinder.quantity().greaterThan(0).or(OrderItemFinder.quantity().lessThan(1000000))));
        toFind.deepFetch(OrderItemFinder.order());
        toFind.size();
    }

    public void testLargeInWithPostDistinct()
    {
        OrderList list = new OrderList();
        for(int i=0;i<1000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription(""+i);
            list.add(order);
        }
        list.bulkInsertAll();
        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(i+15000);
            item.setOrderId((i%1000)+10000);
            itemList.add(item);
        }
        itemList.bulkInsertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            set.add(i+15000);
        }
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.items().id().in(set));
        assertEquals(500, toFind.size());
    }

    public void testLargeInWithPostDistinctForEachWithCursor()
    {
        OrderList list = new OrderList();
        for(int i=0;i<1000;i++)
        {
            Order order = new Order();
            order.setOrderId(i+10000);
            order.setDescription(""+i);
            list.add(order);
        }
        list.bulkInsertAll();
        OrderItemList itemList = new OrderItemList();
        for(int i=0;i<10000;i++)
        {
            OrderItem item = new OrderItem();
            item.setId(i+15000);
            item.setOrderId((i%1000)+10000);
            itemList.add(item);
        }
        itemList.bulkInsertAll();
        IntHashSet set = new IntHashSet();
        for(int i=0;i<8888;i+=2)
        {
            set.add(i+15000);
        }
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        OrderList toFind = new OrderList(OrderFinder.items().id().in(set));
        final int[] count = new int[1];
        toFind.forEachWithCursor(new DoWhileProcedure()
        {
            public boolean execute(Object each)
            {
                count[0]++;
                return true;
            }
        });
        assertEquals(500, count[0]);
    }

    public void testSetLengthZero()
    {
        assertTrue(ParaDeskFinder.activeBoolean().in(new BooleanHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.closedDate().in(new HashSet()) instanceof None);
        assertTrue(ParaDeskFinder.connectionLong().in(new LongHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.createTimestamp().in(new HashSet()) instanceof None);
        assertTrue(ParaDeskFinder.deskIdString().in(new HashSet()) instanceof None);
        assertTrue(ParaDeskFinder.locationByte().in(new ByteHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.maxFloat().in(new FloatHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.minShort().in(new ShortHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.sizeDouble().in(new DoubleHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.statusChar().in(new CharHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.tagInt().in(new IntHashSet()) instanceof None);
        assertTrue(ParaDeskFinder.bigDouble().in(new HashSet<BigDecimal>()) instanceof None);
    }

    public void testSetLengthOne()
    {
        BooleanHashSet set = new BooleanHashSet();
        set.add(true);
        assertTrue(ParaDeskFinder.activeBoolean().in(set) instanceof AtomicEqualityOperation);
        HashSet set2 = new HashSet();
        set2.add(new java.util.Date());
        assertTrue(ParaDeskFinder.closedDate().in(set2) instanceof AtomicEqualityOperation);
        LongHashSet set3 = new LongHashSet();
        set3.add(1);
        assertTrue(ParaDeskFinder.connectionLong().in(set3) instanceof AtomicEqualityOperation);
        HashSet set4 = new HashSet();
        set4.add(new Timestamp(System.currentTimeMillis()));
        assertTrue(ParaDeskFinder.createTimestamp().in(set4) instanceof AtomicEqualityOperation);
        HashSet set5 = new HashSet();
        set5.add("test");
        assertTrue(ParaDeskFinder.deskIdString().in(set5) instanceof AtomicEqualityOperation);
        ByteHashSet set6 = new ByteHashSet();
        set6.add((byte)1);
        assertTrue(ParaDeskFinder.locationByte().in(set6) instanceof AtomicEqualityOperation);
        FloatHashSet set7 = new FloatHashSet();
        set7.add(1.0f);
        assertTrue(ParaDeskFinder.maxFloat().in(set7) instanceof AtomicEqualityOperation);
        ShortHashSet set8 = new ShortHashSet();
        set8.add((short)1);
        assertTrue(ParaDeskFinder.minShort().in(set8) instanceof AtomicEqualityOperation);
        DoubleHashSet set9 = new DoubleHashSet();
        set9.add(1.0d);
        assertTrue(ParaDeskFinder.sizeDouble().in(set9) instanceof AtomicEqualityOperation);
        CharHashSet set10 = new CharHashSet();
        set10.add('x');
        assertTrue(ParaDeskFinder.statusChar().in(set10) instanceof AtomicEqualityOperation);
        IntHashSet set11 = new IntHashSet();
        set11.add(1);
        assertTrue(ParaDeskFinder.tagInt().in(set11) instanceof AtomicEqualityOperation);
        Set<BigDecimal> set12 = new HashSet<BigDecimal>(1);
        set12.add(BigDecimal.ONE);
        assertTrue(ParaDeskFinder.bigDouble().in(set12) instanceof AtomicEqualityOperation);

    }

    public void testMultipleIndenticalInWithLargeIn()
    {
        IntHashSet set = new IntHashSet();
        for(int i=1;i<2000;i++)
        {
            set.add(i);
        }
        IntHashSet products = new IntHashSet();
        products.add(1);
        products.add(2);
        Operation repeatedInOp = OrderFinder.items().productId().in(products);
        Operation op1 = repeatedInOp.and(OrderFinder.items().quantity().greaterThan(2));
        Operation op2 = repeatedInOp.and(OrderFinder.items().state().eq("foo"));
        Operation op = op1.or(op2);
        op = op.and(OrderFinder.orderId().in(set));
        OrderList list = new OrderList(op);
        list.forceResolve();
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

    public void testDeepFetchBypassCache()
    {
        IntHashSet itemIds = createOrdersAndItems();
        Operation op = OrderItemFinder.id().in(itemIds);
        int count = this.dbCalls();
        OrderItemList foundItems = OrderItemFinder.findManyBypassCache(op);
        foundItems.deepFetch(OrderItemFinder.order());
        assertEquals(itemIds.size(), foundItems.size());
        assertEquals(itemIds.size()/2, foundItems.getOrders().size());
        assertEquals(count + 2, this.dbCalls());
    }

    public void testDeepFetchBypassCacheInTransaction()
    {
        final IntHashSet itemIds = createOrdersAndItems();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = OrderItemFinder.id().in(itemIds);
                int count = dbCalls();
                OrderItemList foundItems = OrderItemFinder.findManyBypassCache(op);
                foundItems.deepFetch(OrderItemFinder.order());
                assertEquals(itemIds.size(), foundItems.size());
                assertEquals(itemIds.size()/2, foundItems.getOrders().size());
                assertEquals(count + 2, dbCalls());
                return null;
            }
        });
    }

    public void testDeepFetchWithLargeIn()
    {
        IntHashSet itemIds = createOrdersAndItems();
        OrderFinder.clearQueryCache();
        OrderItemFinder.clearQueryCache();
        checkDeepFetchWithLargeIn(itemIds);
    }

    private void checkDeepFetchWithLargeIn(IntHashSet itemIds)
    {
        Operation op = OrderItemFinder.id().in(itemIds);
        OrderItemList foundItems = OrderItemFinder.findMany(op);
        foundItems.deepFetch(OrderItemFinder.order());
        assertEquals(itemIds.size(), foundItems.size());
        assertEquals(itemIds.size()/2, foundItems.getOrders().size());
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
            items.add(item);

            itemIds.add(i+1000);
            itemIds.add(i+3000);
        }
        items.bulkInsertAll();
        return itemIds;
    }

    public void testDeepFetchWithLargeInWithTransaction()
    {
        final IntHashSet ids = createOrdersAndItems();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                checkDeepFetchWithLargeIn(ids);
                return null;
            }
        });
    }

    public void testTwoLargeInOnRelated()
    {
        IntHashSet set = new IntHashSet();
        for(int i=0;i<2000;i++)
        {
            set.add(i);
        }
        Operation op = OrderFinder.items().orderId().in(set).and(OrderFinder.items().productId().in(set));
        OrderFinder.findMany(op).forceResolve();
    }

    public void testInClauses()
    {
        IntHashSet sources = new IntHashSet();
        sources.add(0);
        sources.add(1);

        IntHashSet pks = new IntHashSet();
        for(int i=0;i<10000;i++) pks.add(i);

        assertTrue(UserFinder.findMany(UserFinder.sourceId().in(sources).and(UserFinder.id().in(pks))).size() > 0);
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

        OrderFinder.clearQueryCache();
        Operation op = OrderFinder.orderId().in(idSet).and(OrderFinder.userId().in(idSet));
        OrderFinder.findMany(op).forceResolve();
    }


    public void testInClauseSubstitutionTwoClauses()
    {
        DoubleHashSet doubleSet = new DoubleHashSet(2000);
        for(int i = 0; i < 2000; i++)
        {
            doubleSet.add(i);
        }

        IntHashSet idSet = createIntHashSet(2000);
        Operation op = OrderFinder.items().quantity().greaterThan(0);
        op = op.and(OrderFinder.items().productId().in(idSet));
        op = op.and(OrderFinder.items().discountPrice().lessThan(10000));
        op = op.and(OrderFinder.items().quantity().in(doubleSet));

        assertEquals(3, OrderFinder.findMany(op).size());
    }

    public void testInClauseSubstitution()
    {
        DoubleHashSet doubleSet = new DoubleHashSet(2000);
        for(int i = 0; i < 2000; i++)
        {
            doubleSet.add(i);
        }

        Operation op = OrderFinder.items().quantity().greaterThan(0);
        op = op.and(OrderFinder.items().discountPrice().lessThan(10000));
        op = op.and(OrderFinder.items().quantity().in(doubleSet));

        assertEquals(3, OrderFinder.findMany(op).size());
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

        OrderFinder.clearQueryCache();
        Operation op = OrderFinder.orderId().in(idSet).and(OrderFinder.userId().in(idSet)).and(OrderFinder.orderStatus().status().in(idSet));
        OrderFinder.findMany(op).forceResolve();
    }

    public void testLargeInWithToOneRelationshipWithOr()
    {
        IntHashSet idSet = createIntHashSet(5000);

        Operation op = OrderItemFinder.order().userId().greaterThan(7).or(OrderItemFinder.order().orderId().in(idSet));
        OrderItemFinder.findMany(op).forceResolve();
    }

    public void testLargeInWithTruncation()
    {
        Set<String> set = UnifiedSet.newSet();
        String longString = "Tracking01234567890"; // more than 15 chars
        assertTrue(longString.length() > OrderFinder.trackingId().getMaxLength());
        for(int i=0;i<1000;i++)
        {
            set.add(longString + i);
        }
        set.add("123");
        set.add("124");
        set.add("125");

        assertEquals(3, OrderFinder.findMany(OrderFinder.trackingId().in(set)).size());
    }

    //the generated sql doesn't work with H2 and Db2. It works with others (ase, postgres, ms sql, iq)
    public void xtestLargeInWithToOneRelationshipWithNotExists()
    {
        IntHashSet idSet = createIntHashSet(5000);
        idSet.remove(1);

        Operation op = OrderItemFinder.order().notExists(OrderFinder.orderId().in(idSet));
        OrderItemList items = OrderItemFinder.findMany(op);
        assertEquals(1, items.size());
        assertEquals(1, items.get(0).getOrderId());
    }

    public void testTwoLargeInWithToOneRelationshipWithOr()
    {
        IntHashSet idSet = createIntHashSet(5000);

        Operation op = OrderItemFinder.order().userId().in(idSet).or(OrderItemFinder.order().orderId().in(idSet));
        OrderItemFinder.findMany(op).forceResolve();
    }

    public void testLargeInWithToManyRelationshipWithOr()
    {
        DoubleHashSet set = new DoubleHashSet();
        for(int i=0;i<5000;i++) set.add(i);

        set.add(15.5);


        Operation op = OrderFinder.items().orderId().eq(1).or(OrderFinder.items().originalPrice().in(set));
        OrderList orders = OrderFinder.findMany(op);
        assertEquals(2, orders.size());
    }

    //the generated sql doesn't work with H2 and Db2. It works with others (ase, postgres, ms sql, iq)
    public void xtestLargeInWithToManyRelationshipTwoDeepWithOr()
    {
        IntHashSet idSet = createIntHashSet(5000);

        idSet.remove(10);


        Operation op = OrderFinder.items().orderId().eq(1).or(OrderFinder.items().orderItemStatus().status().in(idSet));
        OrderList orders = OrderFinder.findMany(op);
        assertEquals(2, orders.size());
    }

    public void testTwoRelationshipsWithOrAndIn()
    {
        IntHashSet idSet = createIntHashSet(5000);

        idSet.remove(10);

        Operation op = OrderFinder.orderStatus().exists().or(OrderFinder.items().orderItemStatus().status().in(idSet));
        OrderList orders = OrderFinder.findMany(op);
        assertEquals(3, orders.size());
    }

    public void testHugeInClauseWithSourelessToSourceJoin() throws Exception
    {
        IntHashSet intHashSet = new IntHashSet();
        for (int i=0;i<1007;i++)
        {
            intHashSet.add(i);
        }
        Operation op = PositionQuantityFinder.product().manufacturerId().in(intHashSet);
        op = op.and(PositionQuantityFinder.acmapCode().eq("A"));
        Timestamp buzDate = new Timestamp(timestampFormat.parse("2010-10-11 00:00:00").getTime());
        op = op.and(PositionQuantityFinder.businessDate().eq(buzDate));
        PositionQuantityList list =  new PositionQuantityList(op);
        list.forceResolve();

        assertEquals(0, list.size());
    }
}
