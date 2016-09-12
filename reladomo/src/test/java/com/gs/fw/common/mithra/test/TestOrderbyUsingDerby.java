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

import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.finder.Operation;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.io.StringWriter;
import java.io.PrintWriter;



public class TestOrderbyUsingDerby extends MithraTestAbstractUsingDerby
{

    protected void setUp() throws Exception
    {
        super.setUp();
        this.setMithraTestObjectToResultSetComparator(new ParaDeskResultSetComparator());
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
                        Trade.class
                };
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
        op = op.and(DatedAccountFinder.deskId().eq("C"));
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
