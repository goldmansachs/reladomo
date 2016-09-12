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

package com.gs.fw.common.mithra.test.h2batch;

import java.sql.SQLException;
import java.util.List;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.databasetype.H2DatabaseType;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.MithraTestAbstract;
import com.gs.fw.common.mithra.test.domain.Book;
import com.gs.fw.common.mithra.test.domain.BookFinder;
import com.gs.fw.common.mithra.test.domain.BookList;


public abstract class H2BatchOperationsTestCasesAbstract
        extends MithraTestAbstract
{
    private static final int NO_BATCH_SIZE = 5;
    private static final int BATCH_SIZE = 2000;
    private static final int INITIAL_VALUE = 54320;
    private static final int INITIAL_ID = 10000;

    protected abstract int getThreshold();

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();

        this.getDatabaseType().setUpdateViaInsertAndJoinThreshold(this.getThreshold());
    }

    public void testInsertAndUpdateNoBatch()
    {
        testInsertAndUpdate(NO_BATCH_SIZE);
    }

    public void testInsertAndUpdateWithBatch()
    {
        testInsertAndUpdate(BATCH_SIZE);
    }

    public void testUpdateOneRowTwoTimesNoBatch() throws SQLException
    {
        this.updateOneRowTwoTimes();
    }

    public void testUpdateOneRowThreeTimesNoBatch() throws SQLException
    {
        this.updateOneRowThreeTimes();
    }

    public void testUpdateOneRowManyTimesWithSameUpdateNoBatch() throws SQLException
    {
        this.updateOneRowManyTimesWithSameUpdate(NO_BATCH_SIZE);
    }

    public void testUpdateOneRowManyTimesWithSameUpdateWithBatch() throws SQLException
    {
        this.updateOneRowManyTimesWithSameUpdate(BATCH_SIZE);
    }

    public void testUpdateOneRowManyTimesWithTwoDifferentUpdatesNoBatch() throws SQLException
    {
        this.updateOneRowManyTimesWithTwoDifferentUpdates(NO_BATCH_SIZE);
    }

    public void testUpdateOneRowManyTimesWithTwoDifferentUpdatesWithBatch() throws SQLException
    {
        this.updateOneRowManyTimesWithTwoDifferentUpdates(BATCH_SIZE);
    }

    public void testIncrementUpdateWithBatch() throws SQLException
    {
        this.incrementUpdate(BATCH_SIZE);
    }

    public void testIncrementUpdateNoBatch() throws SQLException
    {
        this.incrementUpdate(NO_BATCH_SIZE);
    }

    public void testDuplicatesInUpdate()
    {
        this.duplicatesInBatchUpdate(NO_BATCH_SIZE);
    }

    public void testDuplicatesInBatchUpdate()
    {
        this.duplicatesInBatchUpdate(BATCH_SIZE);
    }

    public void testUpdateSameColumnMultipleTimesDifferentValue()
    {
        this.updateSameColumnMultipleTimesDifferentValue(NO_BATCH_SIZE);
    }

    public void testUpdateSameColumnMultipleTimesDifferentValueInBatchUpdate()
    {
        this.updateSameColumnMultipleTimesDifferentValue(BATCH_SIZE);
    }

    public void testUpdateTwoColumnsOneRow()
    {
        this.updateTwoColumnsOneRow();
    }

    public void testMultiUpdateTwoColumns()
    {
        this.multiUpdateTwoColumns(NO_BATCH_SIZE);
    }

    public void testMultiUpdateTwoColumnsInBatchUpdate()
    {
        this.multiUpdateTwoColumns(BATCH_SIZE);
    }

    public void testBatchUpdateTwoColumns()
    {
        this.batchUpdateTwoColumns(NO_BATCH_SIZE);
    }

    public void testBatchUpdateTwoColumnsInBatchUpdate()
    {
        this.batchUpdateTwoColumns(BATCH_SIZE);
    }

    private void testInsertAndUpdate(final int setSize)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        final BookList insertList = createNewBookList(INITIAL_ID, setSize);
                        insertList.insertAll();

                        final Operation op = BookFinder.inventoryId().greaterThanEquals(INITIAL_ID - 10);
                        BookList updateList = new BookList(op);
                        for (int i = 0; i < updateList.size(); i++)
                        {
                            Book allTypes = updateList.get(i);
                            assertEquals(INITIAL_VALUE, allTypes.getInventoryLevel());
                            allTypes.setInventoryLevel(i + 700);
                        }

                        return null;
                    }
                }
        );
        final Operation op = BookFinder.inventoryId().greaterThanEquals(INITIAL_ID - 10);
        List<Book> result = BookFinder.findManyBypassCache(op);
        for (int i = 0; i < setSize; i++)
        {
            assertEquals(i + 700, result.get(i).getInventoryLevel());
        }
    }

    private void updateOneRowTwoTimes() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(orderOne);
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(orderTwo);

                orderOne.setInventoryLevel(100);
                orderOne.setInventoryLevel(300);
                orderTwo.setInventoryLevel(7);
                orderTwo.setInventoryLevel(6);

                return null;
            }
        });

        Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(300, orderOne.getInventoryLevel());
        Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(6, orderTwo.getInventoryLevel());

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(orderOne);
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(orderTwo);

                orderOne.setInventoryLevel(600);
                orderTwo.setInventoryLevel(700);
                orderOne.setInventoryLevel(900);
                orderTwo.setInventoryLevel(9);

                return null;
            }
        });

        orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(900, orderOne.getInventoryLevel());
        orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(9, orderTwo.getInventoryLevel());
    }

    private void updateOneRowThreeTimes() throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(orderOne);
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(orderTwo);

                orderOne.setInventoryLevel(100);
                orderOne.setInventoryLevel(200);
                orderTwo.setInventoryLevel(7);
                orderTwo.setInventoryLevel(8);
                orderOne.setInventoryLevel(250);
                orderTwo.setInventoryLevel(9);
                orderOne.setInventoryLevel(300);
                orderTwo.setInventoryLevel(6);

                return null;
            }
        });

        final Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(300, orderOne.getInventoryLevel());
        final Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(6, orderTwo.getInventoryLevel());
    }

    private void updateOneRowManyTimesWithSameUpdate(final int batchSize) throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(orderOne);
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(orderTwo);

                for (int i = 0; i < batchSize; i++)
                {
                    orderOne.setInventoryLevel(300);
                    orderTwo.setInventoryLevel(6);
                }
                return null;
            }
        });

        final Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(300, orderOne.getInventoryLevel());
        final Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(6, orderTwo.getInventoryLevel());
    }

    private void updateOneRowManyTimesWithTwoDifferentUpdates(final int batchSize) throws SQLException
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(orderOne);
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(orderTwo);

                for (int i = 0; i < batchSize; i++)
                {
                    orderOne.setInventoryLevel(100);
                    orderTwo.setInventoryLevel(7);
                    orderOne.setInventoryLevel(300);
                    orderTwo.setInventoryLevel(6);
                }
                return null;
            }
        });

        final Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(300, orderOne.getInventoryLevel());
        final Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(6, orderTwo.getInventoryLevel());
    }

    private void incrementUpdate(final int count) throws SQLException
    {
        final BookList insertList = createNewBookList(INITIAL_ID, count + 1);
        insertList.insertAll();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book orderOne = BookFinder.findOne(BookFinder.inventoryId().eq(INITIAL_ID + 1));
                final Book orderTwo = BookFinder.findOne(BookFinder.inventoryId().eq(INITIAL_ID + 2));
                final Book orderThree = BookFinder.findOne(BookFinder.inventoryId().eq(INITIAL_ID + 3));

                for (int i = 0; i < count; i++)
                {
                    orderThree.setInventoryLevel(orderThree.getInventoryLevel() + 1);

                    orderOne.setInventoryLevel(orderTwo.getInventoryLevel() + 100);
                    orderTwo.setInventoryLevel(orderOne.getInventoryLevel() + 10);
                }
                return null;
            }
        });

        final Book orderThree = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(INITIAL_ID + 3));
        assertEquals(INITIAL_VALUE + count, orderThree.getInventoryLevel());

        final Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(INITIAL_ID + 1));
        assertEquals(INITIAL_VALUE + count * 100 + (count - 1) * 10, orderOne.getInventoryLevel());
        final Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(INITIAL_ID + 2));
        assertEquals(INITIAL_VALUE + count * 100 + count * 10, orderTwo.getInventoryLevel());
    }

    private void duplicatesInBatchUpdate(final int count)
    {
        final int initialId = 10000;

        final BookList insertList = createNewBookList(initialId, count);
        insertList.insertAll();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        final Operation op1 = BookFinder.inventoryId().greaterThanEquals(10000);
                        BookList updateList1 = new BookList(op1);

                        final Operation op2 = BookFinder.inventoryId().greaterThanEquals(10001);
                        BookList updateList2 = new BookList(op2);

                        for (int i = 0; i < count - 1; i++)
                        {
                            Book obj1 = updateList1.get(i);
                            Book obj2 = updateList2.get(i);

                            obj1.setInventoryLevel(i + 7);
                            obj2.setInventoryLevel(i + 8);
                        }

                        return null;
                    }
                }
        );

        BookList orderOne = BookFinder.findManyBypassCache(BookFinder.inventoryId().greaterThanEquals(10001));
        for (int i = 0; i < count - 1; i++)
        {
            assertEquals(i + 8, orderOne.get(i).getInventoryLevel());
        }

        assertEquals(7, BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(10000)).getInventoryLevel());
    }

    private void updateSameColumnMultipleTimesDifferentValue(final int count)
    {
        final int initialId = 10000;

        final BookList insertList = createNewBookList(initialId, count);
        insertList.insertAll();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        final Operation op1 = BookFinder.inventoryId().greaterThanEquals(9876);
                        BookList updateList1 = new BookList(op1);

                        for (int i = 0; i < count; i++)
                        {
                            Book obj1 = updateList1.get(i);

                            obj1.setInventoryLevel(i);
                        }

                        for (int i = 0; i < count; i++)
                        {
                            Book obj1 = updateList1.get(i);

                            obj1.setInventoryLevel(i + 7);
                        }

                        return null;
                    }
                }
        );

        BookList orderOne = BookFinder.findManyBypassCache(BookFinder.inventoryId().greaterThanEquals(9876));
        for (int i = 0; i < count - 1; i++)
        {
            assertEquals(i + 7, orderOne.get(i).getInventoryLevel());
        }
    }

    private void updateTwoColumnsOneRow()
    {
        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book order = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(order);

                order.setInventoryLevel(1000);
                order.setDescription("Test1");
                order.setInventoryLevel(2000);
                order.setDescription("Test2");

                return null;
            }
        });
        BookFinder.clearQueryCache();
        Book orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(2000, orderOne.getInventoryLevel());
        assertEquals("Test2", orderOne.getDescription());

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book order = BookFinder.findOne(BookFinder.inventoryId().eq(1));
                assertNotNull(order);

                order.setInventoryLevel(3000);
                order.setDescription("Test3");
                order.setInventoryLevel(4000);

                return null;
            }
        });
        BookFinder.clearQueryCache();
        orderOne = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(1));
        assertEquals(4000, orderOne.getInventoryLevel());
        assertEquals("Test3", orderOne.getDescription());

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final Book order = BookFinder.findOne(BookFinder.inventoryId().eq(2));
                assertNotNull(order);

                order.setInventoryLevel(2000);
                order.setDescription("Test4");
                order.setInventoryLevel(5000);

                return null;
            }
        });
        BookFinder.clearQueryCache();
        Book orderTwo = BookFinder.findOneBypassCache(BookFinder.inventoryId().eq(2));
        assertEquals(5000, orderTwo.getInventoryLevel());
        assertEquals("Test4", orderTwo.getDescription());
    }

    private void multiUpdateTwoColumns(int count)
    {
        BookFinder.clearQueryCache();
        final int initialId = 10000;

        final BookList insertList = createNewBookList(initialId, count);
        insertList.insertAll();

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));

                orders.setInventoryLevel(1000);
                orders.setDescription("Test1");
                orders.setInventoryLevel(2000);
                orders.setDescription("Test2");

                return null;
            }
        });

        BookFinder.clearQueryCache();
        BookList orderList1 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        for (int i = 0; i < orderList1.size(); i++)
        {
            assertEquals(2000, orderList1.get(i).getInventoryLevel());
            assertEquals("Test2", orderList1.get(i).getDescription());
        }

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));

                orders.setInventoryLevel(3000);
                orders.setDescription("Test3");
                orders.setInventoryLevel(4000);

                return null;
            }
        });

        BookFinder.clearQueryCache();
        BookList orderList2 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        for (int i = 0; i < orderList2.size(); i++)
        {
            assertEquals(4000, orderList2.get(i).getInventoryLevel());
            assertEquals("Test3", orderList2.get(i).getDescription());
        }

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));

                orders.setInventoryLevel(2000);
                orders.setDescription("Test4");
                orders.setInventoryLevel(5000);

                return null;
            }
        });
        BookFinder.clearQueryCache();
        BookList orderList3 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        for (int i = 0; i < orderList3.size(); i++)
        {

            assertEquals(5000, orderList3.get(i).getInventoryLevel());
            assertEquals("Test4", orderList3.get(i).getDescription());
        }

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));

                orders.setInventoryLevel(6000);
                orders.setDescription("Test4");
                orders.setInventoryLevel(7000);

                return null;
            }
        });
        BookFinder.clearQueryCache();
        BookList orderList4 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        for (int i = 0; i < orderList3.size(); i++)
        {

            assertEquals(7000, orderList4.get(i).getInventoryLevel());
            assertEquals("Test4", orderList4.get(i).getDescription());
        }
    }

    private void batchUpdateTwoColumns(int count)
    {
        BookFinder.clearQueryCache();
        final int initialId = 10000;

        final BookList insertList = createNewBookList(initialId, count);
        insertList.insertAll();

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
                orders.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());

                for (int i = 0; i < orders.size(); i++)
                {
                    orders.get(i).setInventoryLevel(i);
                    orders.get(i).setDescription("Test" + (i + 1));
                    orders.get(i).setInventoryLevel(i + 1);
                    orders.get(i).setDescription("Test" + i);
                }

                return null;
            }
        });

        BookFinder.clearQueryCache();
        BookList orderList1 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        orderList1.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());
        for (int i = 0; i < orderList1.size(); i++)
        {
            assertEquals(i + 1, orderList1.get(i).getInventoryLevel());
            assertEquals("Test" + i, orderList1.get(i).getDescription());
        }
        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
                orders.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());
                for (int i = 0; i < orders.size(); i++)
                {
                    orders.get(i).setInventoryLevel(i + 3);
                    orders.get(i).setDescription("Test" + (i + 3));
                    orders.get(i).setInventoryLevel(i + 4);
                }

                return null;
            }
        });

        BookFinder.clearQueryCache();
        BookList orderList2 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        orderList2.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());
        for (int i = 0; i < orderList2.size(); i++)
        {
            assertEquals(i + 4, orderList2.get(i).getInventoryLevel());
            assertEquals("Test" + (i + 3), orderList2.get(i).getDescription());
        }

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
                orders.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());

                for (int i = 0; i < orders.size(); i++)
                {
                    orders.get(i).setInventoryLevel(i + 5);
                    orders.get(i).setDescription("Test" + (i + 4));
                    orders.get(i).setInventoryLevel(i + 6);
                }

                return null;
            }
        });
        BookFinder.clearQueryCache();
        BookList orderList3 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        orderList3.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());
        for (int i = 0; i < orderList3.size(); i++)
        {

            assertEquals(i + 6, orderList3.get(i).getInventoryLevel());
            assertEquals("Test" + (i + 4), orderList3.get(i).getDescription());
        }

        BookFinder.clearQueryCache();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            @Override
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final BookList orders = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
                orders.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());

                for (int i = 0; i < orders.size(); i++)
                {
                    orders.get(i).setInventoryLevel(i + 6);
                    orders.get(i).setDescription("Test" + (i + 5));
                }

                return null;
            }
        });
        BookFinder.clearQueryCache();
        BookList orderList4 = BookFinder.findMany(BookFinder.inventoryId().greaterThanEquals(initialId));
        orderList4.addOrderBy(BookFinder.inventoryId().ascendingOrderBy());
        for (int i = 0; i < orderList4.size(); i++)
        {

            assertEquals(i + 6, orderList4.get(i).getInventoryLevel());
            assertEquals("Test" + (i + 5), orderList4.get(i).getDescription());
        }
    }

    private BookList createNewBookList(int firstId, long count)
    {
        BookList list = new BookList();
        for (int i = firstId; i < (firstId + count); i++)
        {
            Book book = this.createNewBook(i);
            book.setInventoryLevel(INITIAL_VALUE);
            list.add(book);
        }
        return list;
    }

    private Book createNewBook(int id)
    {
        Book aBook = new Book();
        aBook.setInventoryId(id);
        return aBook;
    }
}

