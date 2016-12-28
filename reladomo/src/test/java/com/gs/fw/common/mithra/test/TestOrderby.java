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

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.test.domain.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TestOrderby
extends MithraTestAbstract
{

    protected void setUp() throws Exception
    {
        super.setUp();
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ParaDesk.class,
            InventoryItem.class,
            SupplierInventoryItem.class,
            Manufacturer.class,
            Supplier.class,
            DatedPnlGroup.class,
            Location.class,
            Book.class,
            DatedTrial.class,
            DatedAccount.class,
            Trade.class,
            Order.class,
            Projito.class,
            ProjitoMeasureOfSuccess.class
        };
    }

    public void testOrderByEachType()
    throws Exception
    {
        this.setMithraTestObjectToResultSetComparator(new ParaDeskResultSetComparator());

        String sql;
        ParaDeskList desks;

        //Boolean
        sql= "select * from PARA_DESK order by ACTIVE_BOOLEAN ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.activeBoolean().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by ACTIVE_BOOLEAN DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.activeBoolean().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Byte
        sql = "select * from PARA_DESK order by LOCATION_BYTE ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.locationByte().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by LOCATION_BYTE DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.locationByte().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Char
        sql = "select * from PARA_DESK order by STATUS_CHAR ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.statusChar().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by STATUS_CHAR DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.statusChar().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Date
        sql = "select * from PARA_DESK order by CLOSED_DATE ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.closedDate().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by CLOSED_DATE DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.closedDate().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Double
        sql = "select * from PARA_DESK order by SIZE_DOUBLE ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.sizeDouble().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by SIZE_DOUBLE DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.sizeDouble().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Float
        sql = "select * from PARA_DESK order by MAX_FLOAT ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.maxFloat().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by MAX_FLOAT DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.maxFloat().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Integer
        sql = "select * from PARA_DESK order by TAG_INT ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.tagInt().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by TAG_INT DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.tagInt().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Long
        sql = "select * from PARA_DESK order by CONNECTION_LONG ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.connectionLong().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by CONNECTION_LONG DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.connectionLong().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Short
        sql = "select * from PARA_DESK order by MIN_SHORT ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.minShort().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by MIN_SHORT DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.minShort().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //String
        sql = "select * from PARA_DESK order by DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.deskIdString().ascendingOrderBy());
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by DESK_ID_STRING DESC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.deskIdString().descendingOrderBy());
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        //Timestamp
        sql = "select * from PARA_DESK order by CREATE_TIMESTAMP ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.createTimestamp().ascendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by CREATE_TIMESTAMP DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.createTimestamp().descendingOrderBy().and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());
    }

    public void testChainedOrderBy()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        sql = "select * from PARA_DESK order by STATUS_CHAR ASC, SIZE_DOUBLE DESC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.statusChar().ascendingOrderBy().and(ParaDeskFinder.sizeDouble().descendingOrderBy()).and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());

        sql = "select * from PARA_DESK order by STATUS_CHAR DESC, SIZE_DOUBLE ASC, DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.statusChar().descendingOrderBy().and(ParaDeskFinder.sizeDouble().ascendingOrderBy()).and(ParaDeskFinder.deskIdString().ascendingOrderBy()));
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());
    }

	public void testDatedOrderBy()
	{
		Timestamp asOf = new Timestamp(System.currentTimeMillis());
		DatedAccountFinder.DatedAccountSingleFinder finder = DatedAccountFinder.getFinderInstance();
		DatedAccountList list = new DatedAccountList(
				finder.datedTrial().name().eq("001A").and(
				finder.businessDate().eq(asOf).and(
				finder.deskId().eq("A"))
		));
		list.setOrderBy(finder.datedPnlGroup().name().ascendingOrderBy());
		list.forceResolve();
	}

    public void testEmptyOrderBy()
    {
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.maxFloat().eq(-1234567000));
        list.setOrderBy(ParaDeskFinder.maxFloat().ascendingOrderBy());
        assertEquals(0, list.size());
    }

    public void testCalculatedStringOrderBy()
    {
        Order order = new Order();
        order.setOrderId(1000);
        order.setDescription("first murder"); // murder comes before order
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setState("xyz");
        order.insert();

        OrderList orderList = new OrderList(OrderFinder.all());
        orderList.setOrderBy(OrderFinder.description().toLowerCase().ascendingOrderBy());

        assertTrue(orderList.size() > 1);
        assertEquals(1000, orderList.get(0).getOrderId());

        orderList = new OrderList(OrderFinder.all());
        orderList.setOrderBy(OrderFinder.description().toLowerCase().descendingOrderBy());

        assertEquals(1000, orderList.get(orderList.size() - 1).getOrderId());
    }

    public void testSingleOrderBy()
    {
        ParaDeskList list = new ParaDeskList(ParaDeskFinder.all());
        assertTrue(list.size() > 0); // make sure it's in cache
        list = new ParaDeskList(ParaDeskFinder.deskIdString().eq("rnd"));
        list.setOrderBy(ParaDeskFinder.maxFloat().ascendingOrderBy());
        assertEquals(1, list.size());
    }

    public void testReadOnlyOrderByPostResolve()
    throws Exception
    {
        String sql;
        ParaDeskList desks;

        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.forceResolve();

        sql = "select * from PARA_DESK order by DESK_ID_STRING ASC";
        desks = new ParaDeskList(ParaDeskFinder.all());
        desks.setOrderBy(ParaDeskFinder.deskIdString().ascendingOrderBy());
        this.orderedRetrievalTest(sql, desks, new ParaDeskResultSetComparator());
    }

    public void testTransactionalOrderByPostResolve()
    throws Exception
    {
        String sql;
        OrderList orders;

        orders = new OrderList(OrderFinder.all());
        orders.forceResolve();

        sql = "select * from APP.ORDERS order by DESCRIPTION ASC";
        orders = new OrderList(OrderFinder.all());
        orders.setOrderBy(OrderFinder.description().ascendingOrderBy());
        this.orderedRetrievalTest(sql, orders, new OrderTestResultSetComparator());
    }

    public void testOrderBySettingLimit()
    {
        OrderList orderList = new OrderList(OrderFinder.orderId().lessThan(10));
        orderList.setOrderBy(OrderFinder.orderId().descendingOrderBy());
        orderList.setMaxObjectsToRetrieve(1);

//        assertEquals(1, orderList.size());
        Order order = orderList.get(0);
        assertEquals(4, order.getOrderId());
    }

    public void testDeepOrderByWithObjectInQuery() throws SQLException
    {
        InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
        String location = "New York";
        BookList list = new BookList(itemFinder.manufacturer().location().city().eq(location)
                .and(itemFinder.suppliers().location().eq(location)));

        list.setOrderBy(itemFinder.manufacturer().location().city().descendingOrderBy());
        list.forceResolve();
    }

    public void testDeepOrderByWithoutObjectInQuery() throws SQLException
    {
        InventoryItemFinder itemFinder = BookFinder.getFinderInstance();
        String location = "New York";
        BookList list = new BookList(itemFinder.suppliers().location().eq(location));
        list.setOrderBy(itemFinder.manufacturer().location().city().descendingOrderBy());
        list.forceResolve();
    }

    public void testDatedDeepOrderByWithoutObject()
    {
        Operation op = DatedAccountFinder.name().eq("743737222");
        op = op.and(DatedAccountFinder.deskId().eq("A"));
        DatedAccountList list = new DatedAccountList(op);
        list.setOrderBy(DatedAccountFinder.datedTrial().name().ascendingOrderBy());
        assertEquals(1, list.size());
    }

    public void testMapperStacks() throws Exception
    {
        assertTrue(MultiThreadingUtil.runMultithreadedTest(3, 100000, new MultiThreadingUtil.FailureHandler()
        {
            public String handleFailure(Throwable t)
            {
                t.printStackTrace();
                StringWriter stackTraceString = new StringWriter();
                t.printStackTrace(new PrintWriter(stackTraceString));
                return t.toString();
            }
        },
         createRunnable(),createRunnable(),createRunnable()));
    }

    public void testMultipleOrderBys()
    {
        Projito projito = ProjitoFinder.findOne(ProjitoFinder.id().eq(2));
        assertNotNull(projito);

        ProjitoMeasureOfSuccessList measuresOfSuccess = projito.getMeasuresOfSuccess();
        OrderBy orderBy = measuresOfSuccess.getOrderBy();
        assertNotNull(orderBy);

        assertEquals("MoS 1",measuresOfSuccess.get(0).getName());
    }

    public void testWithOneThread()
    {
        this.exerciseMapperStack();
    }

    private Runnable createRunnable()
    {
        return new Runnable()
        {
            public void run()
            {
                exerciseMapperStack();
            }
        };
    }

    private void exerciseMapperStack()
    {
        Timestamp fromDate = Timestamp.valueOf("2007-05-15 6:25:00");
        Timestamp toDate = Timestamp.valueOf("2007-05-16 7:28:00");
        Timestamp lewBusinessDate = Timestamp.valueOf("2007-05-15 23:59:00.000");

        Operation op = TradeFinder.businessDate().eq(lewBusinessDate);
        Operation returnOp = TradeFinder.processingDateFrom().greaterThan(fromDate);
        returnOp = returnOp.and(TradeFinder.processingDateFrom().lessThanEquals(toDate));
        returnOp = returnOp.and(TradeFinder.processingDate().equalsEdgePoint());
        op = op.and(returnOp);

        TradeList updateOrInsertTrades = new TradeList(op);
        TradeFinder.TradeCollectionFinderForRelatedClasses incrementalTradeRelation = TradeFinder.tradesByTradeRef(lewBusinessDate, toDate);
        updateOrInsertTrades.addOrderBy(incrementalTradeRelation.tradeRef().ascendingOrderBy());
        updateOrInsertTrades.size();
    }
}
