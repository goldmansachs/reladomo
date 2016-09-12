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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import com.gs.fw.common.mithra.util.DoWhileProcedure;

import java.sql.Timestamp;
import java.util.*;



public class TestCursor extends MithraTestAbstract
{
    private Comparator bookComparator = BookFinder.inventoryId().descendingOrderBy();
    private Comparator datedEntityComparator = DatedEntityFinder.id().descendingOrderBy();
    private Comparator accountComparator = AccountFinder.accountNumber().descendingOrderBy();

    public Class[] getRestrictedClassList()
    {
        return new Class[]
                {
                        Book.class,
                        DatedEntity.class,
                        Account.class,
                        Order.class
                };
    }

    public static class ClosureWithState implements DoWhileProcedure
    {
        private int count = 0;
        private boolean exception;
        private int stop = -1;
        private List result = new ArrayList();

        public ClosureWithState()
        {
            this.stop = -1;
            this.exception = false;
        }

        public ClosureWithState(int stop)
        {
            this.stop = stop;
            this.exception = false;
        }

        public ClosureWithState(int stop, boolean exception)
        {
            this.stop = stop;
            this.exception = exception;
        }

        public boolean execute(Object input)
        {
            this.count++;
            if (this.count == this.stop)
            {
                if (this.exception)
                {
                    throw new RuntimeException("Exception used for test.");
                }
                else
                {
                    return false;
                }
            }
            result.add(input);
            return true;
        }

        public int getCount()
        {
            return this.count;
        }

        public List getResult()
        {
            return this.result;
        }
    }

    private void compareList(List list1, List list2, Comparator comparator)
    {
        int size1 = list1.size();
        int size2 = list2.size();
        assertEquals(size1, size2);
        Collections.sort(list1, comparator);
        Collections.sort(list2, comparator);
        Iterator it1 = list1.iterator();
        Iterator it2 = list2.iterator();
        while (it1.hasNext())
        {
            Object o1 = it1.next();
            Object o2 = it2.next();
            assertEquals(0, comparator.compare(o1, o2));
        }
    }

    private BookList createBookList()
    {
        BookList list = new BookList();
        Book a = new Book();
        a.setDescription("'a' description");
        a.setInventoryId(10001);
        Book b = new Book();
        b.setDescription("'b' description");
        b.setInventoryId(10002);
        Book c = new Book();
        c.setDescription("'c' description");
        c.setInventoryId(10003);
        list.add(a);
        list.add(b);
        list.add(c);
        return list;
    }

    private DatedEntityList createDatedEntityList()
    {
        DatedEntity a = new DatedEntity(new Timestamp(new Date().getTime()));
        a.setId(2);
        DatedEntity b = new DatedEntity(new Timestamp(new Date().getTime()));
        b.setId(3);
        DatedEntityList list = new DatedEntityList();
        list.add(a);
        list.add(b);
        return list;
    }

    // The unit test will use the same code to test partial and full cache.

    public void testCursorIterationNonDatedOperationBasedList() throws Exception
    {
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        ClosureWithState cl = new ClosureWithState();
        list.forEachWithCursor(cl);
        this.compareList(list, cl.getResult(), bookComparator);
    }

    public void testCursorIterationNonDatedNonOperationBasedList() throws Exception
    {
        BookList bookList = this.createBookList();
        ClosureWithState cl = new ClosureWithState();
        bookList.forEachWithCursor(cl);
        this.compareList(bookList, cl.getResult(), bookComparator);
    }

    public void testCursorIterationDatedOperationBasedList() throws Exception
    {
        Operation op = DatedEntityFinder.processingDate().eq(new Date());
        DatedEntityList list = new DatedEntityList(op);
        ClosureWithState cl = new ClosureWithState();
        list.forEachWithCursor(cl);
        this.compareList(list, cl.getResult(), datedEntityComparator);
    }

    public void testCursorIterationDatedNonOperationBasedList() throws Exception
    {
        DatedEntityList datedEntityList = this.createDatedEntityList();
        ClosureWithState cl = new ClosureWithState();
        datedEntityList.forEachWithCursor(cl);
        this.compareList(datedEntityList, cl.getResult(), datedEntityComparator);
    }

    public void testCursorWithPostLoadOperation() throws Exception
    {
        BookFinder.clearQueryCache();
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        ClosureWithState cl = new ClosureWithState(2);
        list.forEachWithCursor(cl, BookFinder.author().startsWith("Jo"));
        assertEquals(1, cl.getCount());
    }

    public void testCursorIterationOperationBasedWithBreak() throws Exception
    {
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        ClosureWithState cl = new ClosureWithState(2);
        list.forEachWithCursor(cl);
        assertEquals(2, cl.getCount());
    }

    public void testCursorIterationNonOperationBasedWithBreak() throws Exception
    {
        BookList bookList = this.createBookList();
        ClosureWithState cl = new ClosureWithState(2);
        bookList.forEachWithCursor(cl);
        assertEquals(2, cl.getCount());
    }

    public void testCursorIterationOperationBasedWithExceptionAndConnectionCheck() throws Exception
    {
        int activeConnections = ConnectionManagerForTests.getInstance().getNumberOfActiveConnections();
        testCursorIterationOperationBasedWithException();
        int afterExceptionActiveConnections = ConnectionManagerForTests.getInstance().getNumberOfActiveConnections();
        assertEquals(activeConnections, afterExceptionActiveConnections);
    }

    public void testCursorIterationOperationBasedWithException()
    {
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        ClosureWithState cl = new ClosureWithState(2, true);
        try
        {
            list.forEachWithCursor(cl);
        }
        catch (RuntimeException e)
        {
        }
    }

    public void testCursorIterationWithDeepFetch() throws Exception
    {
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        list.deepFetch(BookFinder.manufacturer());
        ClosureWithState cl = new ClosureWithState();
        try
        {
            list.forEachWithCursor(cl);
            fail();
        }
        catch (MithraBusinessException e)
        {
        }
    }

    public void testCursorIterationWithMultipleDesks() throws Exception
    {
        Set deskIdSet = new HashSet();
        deskIdSet.add("A");
        deskIdSet.add("B");
        Operation op = AccountFinder.deskId().in(deskIdSet);
        op = op.and(AccountFinder.pnlGroupId().eq("999A"));

        AccountList list = new AccountList(op);
        ClosureWithState cl = new ClosureWithState();
        list.forEachWithCursor(cl);
        this.compareList(list, cl.getResult(), accountComparator);
    }

    public void testCursorWithLargeInClauseAndEmptyTable() throws Exception
    {
        IntHashSet set = new IntHashSet();
        for (int i = 0; i < 1000000; i++)
        {
            set.add(i);
        }
        OrderFinder.findMany(OrderFinder.all()).deleteAll();
        OrderFinder.findMany(OrderFinder.orderId().in(set)).forEachWithCursor(new DoWhileProcedure()
        {
            public boolean execute(Object o)
            {
                fail();
                return true;
            }
        });
    }

    public void testCursorWithLargeResult()
    {
        testCursorWithLargeResult(1);
    }

    public void testCursorWithLargeResult(int threads)
    {
        insertManyOrders();
        OrderList cursorList1 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999));
        cursorList1.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        cursorList1.setNumberOfParallelThreads(threads);
        cursorList1.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 1000;

            public boolean execute(Order o)
            {
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                start++;
                return true;
            }
        });

        cursorList1 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999).and(OrderFinder.orderId().lessThan(2000)));
        cursorList1.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        cursorList1.setNumberOfParallelThreads(threads);
        cursorList1.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 1000;

            public boolean execute(Order o)
            {
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                start++;
                return true;
            }
        });

        cursorList1 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999).and(OrderFinder.orderId().lessThan(3000)));
        cursorList1.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        cursorList1.setNumberOfParallelThreads(threads);
        cursorList1.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 1000;

            public boolean execute(Order o)
            {
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                start++;
                return true;
            }
        });

        OrderList cursorList2 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999));
        cursorList2.setOrderBy(OrderFinder.orderId().descendingOrderBy());
        cursorList2.setNumberOfParallelThreads(threads);
        cursorList2.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 11100;

            public boolean execute(Order o)
            {
                start--;
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                return true;
            }
        });
        assertEquals(5100, OrderFinder.findMany(OrderFinder.orderId().greaterThan(5999)).size());
    }

    public void testCursorWithLargeResultAndAborted()
    {
        insertManyOrders();
        OrderList cursorList1 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999));
        cursorList1.setOrderBy(OrderFinder.orderId().ascendingOrderBy());
        cursorList1.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 1000;

            public boolean execute(Order o)
            {
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                start++;
                return start < 3000;
            }
        });

        OrderList cursorList2 = OrderFinder.findMany(OrderFinder.orderId().greaterThan(999));
        cursorList2.setOrderBy(OrderFinder.orderId().descendingOrderBy());
        cursorList2.forEachWithCursor(new DoWhileProcedure<Order>()
        {
            private int start = 11100;

            public boolean execute(Order o)
            {
                start--;
                assertEquals(start, o.getOrderId());
                assertEquals(start + 6000, o.getUserId());
                assertEquals("T" + start, o.getTrackingId());
                return start > 5000;
            }
        });
        assertEquals(5100, OrderFinder.findMany(OrderFinder.orderId().greaterThan(5999)).size());
    }

    private void insertManyOrders()
    {
        OrderList orderList = new OrderList();
        for (int i = 0; i < 10100; i++)
        {
            Order order = new Order();
            order.setOrderId(i + 1000);
            order.setDescription("order number " + i);
            order.setUserId(i + 7000);
            order.setOrderDate(new Timestamp(System.currentTimeMillis()));
            order.setTrackingId("T" + (i + 1000));
            orderList.add(order);
        }
        orderList.bulkInsertAll();
    }

    public void testCursorInTransactionIterationNonDatedOperationBasedList() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationNonDatedNonOperationBasedList();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationNonDatedNonOperationBasedList() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationNonDatedNonOperationBasedList();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationDatedOperationBasedList() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationDatedOperationBasedList();
                return null;
            }
        });
    }

    public void testCursorInTransactionWithPostLoadOperation() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorWithPostLoadOperation();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationDatedNonOperationBasedList() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationDatedNonOperationBasedList();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationOperationBasedWithBreak() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationOperationBasedWithBreak();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationNonOperationBasedWithBreak() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationNonOperationBasedWithBreak();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationWithDeepFetch() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationWithDeepFetch();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationWithMultipleDesks() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationWithMultipleDesks();
                return null;
            }
        });
    }

    public void testCursorInTransactionWithLargeInClauseAndEmptyTable() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorWithLargeInClauseAndEmptyTable();
                return null;
            }
        });
    }

    public void testCursorInTransactionWithLargeResult() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorWithLargeResult();
                return null;
            }
        });
    }

    public void testCursorInTransactionWithLargeResultAndAborted() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorWithLargeResultAndAborted();
                return null;
            }
        });
    }

    public void testCursorInTransactionIterationOperationBasedWithException() throws Exception
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCursorIterationOperationBasedWithException();
                return null;
            }
        });
    }

    public void testCursorIncrementsDatabaseRetrieveCount() throws Exception
    {
        Operation op = BookFinder.all();
        BookList list = new BookList(op);
        int countBefore = this.getRetrievalCount();
        list.forEachWithCursor(new DoWhileProcedure()
        {
            @Override
            public boolean execute(Object o)
            {
                return true;
            }
        });
        int expectedCount = BookFinder.getMithraObjectPortal().isPartiallyCached() ? 1 : 0;
        assertEquals(expectedCount, this.getRetrievalCount() - countBefore);
    }
}
