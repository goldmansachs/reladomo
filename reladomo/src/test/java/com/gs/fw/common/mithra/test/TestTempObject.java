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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.MithraUniqueIndexViolationException;
import com.gs.fw.common.mithra.TemporaryContext;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.Order;
import com.gs.fw.common.mithra.test.domain.OrderDriver;
import com.gs.fw.common.mithra.test.domain.OrderDriverFinder;
import com.gs.fw.common.mithra.test.domain.OrderDriverList;
import com.gs.fw.common.mithra.test.domain.OrderFinder;
import com.gs.fw.common.mithra.test.domain.OrderItem;
import com.gs.fw.common.mithra.test.domain.OrderItemDatabaseObject;
import com.gs.fw.common.mithra.test.domain.OrderItemFinder;
import com.gs.fw.common.mithra.test.domain.OrderItemList;
import com.gs.fw.common.mithra.test.domain.OrderList;
import com.gs.fw.common.mithra.test.domain.ParaProductDriver;
import com.gs.fw.common.mithra.test.domain.ParaProductDriverFinder;
import com.gs.fw.common.mithra.test.domain.ParaProductDriverList;
import com.gs.fw.common.mithra.test.domain.ParaProductFinder;
import com.gs.fw.common.mithra.test.domain.ParaProductList;
import com.gs.fw.common.mithra.test.domain.PositionDriver;
import com.gs.fw.common.mithra.test.domain.PositionDriverFinder;
import com.gs.fw.common.mithra.test.domain.PositionDriverList;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryFinder;
import com.gs.fw.common.mithra.test.domain.adjustmenthistory.PositionAdjustmentHistoryList;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityFinder;
import com.gs.fw.common.mithra.test.domain.desk.balance.position.PositionQuantityList;
import com.gs.fw.common.mithra.util.AutoShutdownThreadExecutor;
import com.gs.fw.common.mithra.util.ExceptionHandlingTask;
import com.gs.fw.common.mithra.util.MithraMultiThreadedLoader;
import com.gs.fw.common.mithra.util.ThreadConservingExecutor;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class TestTempObject extends MithraTestAbstract
{

    public void testClearCache()
    {
        PositionDriverFinder.clearQueryCache();
        TemporaryContext temporaryContext = createPositionDrivers("A");
        PositionDriverFinder.clearQueryCache();
        temporaryContext.destroy();
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        temporaryContext = createPositionDrivers("A");
        MithraManagerProvider.getMithraManager().clearAllQueryCaches();
        temporaryContext.destroy();
    }

    public void testCreateAndDestoryWithSource()
    {
        TemporaryContext temporaryContext = createPositionDrivers("A");
        temporaryContext.destroy();
    }

    public void testRollback()
    {
        final List<Exception> exceptionsList = FastList.newList();
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();

                try
                {
                    Order order = new Order();
                    order.setOrderId(1000);
                    order.insert();
                    if (exceptionsList.isEmpty())
                    {
                        MithraBusinessException exception = new MithraBusinessException("Exception");
                        exception.setRetriable(true);
                        exceptionsList.add(exception);
                        throw exception;
                    }
                    return null;
                }
                finally
                {
                    temporaryContext.destroy();
                }
            }
        }, 5);
        assertNotNull(OrderFinder.findOne(OrderFinder.orderId().eq(1000)));
    }

    public void testComplexRollback()
    {
        try
        {
            MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();

                    try
                    {
                        Order order = new Order();
                        order.setOrderId(1000);
                        order.insert();
                        OrderItem item = new OrderItem();
                        item.setId(1);
                        item.insert();
                        OrderFinder.findMany(OrderFinder.all()).forceResolve();
                        tx.executeBufferedOperations();
                    }
                    finally
                    {
                        temporaryContext.destroy();
                    }
                    return null;
                }
            }, 1);
            fail("Exception should have been thrown");
        }
        catch (MithraBusinessException e)
        {
            assertTrue(e.getMessage().contains(OrderItemDatabaseObject.class.getName()));
        }
    }

    public void testRollbackWithTempContext() throws Exception
    {
        OrderList list = new OrderList(OrderFinder.all());
        this.executeTransactionWithTempContextThatRollbacks(list);
    }

    public void testEnsureQueryCacheGetsCleanWhenDestroyContext()
    {
        TemporaryContext context = ParaProductDriverFinder.createTemporaryContext("A");
        ParaProductDriverList drivers = new ParaProductDriverList();
        drivers.add(createParaProductDriver("A","ABC123","12345"));
        drivers.insertAll();

        Operation op = ParaProductDriverFinder.existsWithJoin(ParaProductFinder.acmapCode(), ParaProductFinder.gsn(), ParaProductFinder.cusip());
        op = op.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products = new ParaProductList(op);
        assertEquals(0, products.size());
        context.destroy();

        TemporaryContext context2 = ParaProductDriverFinder.createTemporaryContext("A");
        ParaProductDriverList drivers2 = new ParaProductDriverList();
        drivers2.add(createParaProductDriver("A","ABC124","12346"));
        drivers2.insertAll();

        Operation op2 = ParaProductDriverFinder.existsWithJoin(ParaProductFinder.acmapCode(), ParaProductFinder.gsn(), ParaProductFinder.cusip());
        op2 = op2.and(ParaProductFinder.acmapCode().eq("A"));
        ParaProductList products2 = new ParaProductList(op2);
        assertEquals(1, products2.size());
        context2.destroy();
    }

    public void testCreateAndDestroyWithSourceTwice()
    {
        TemporaryContext temporaryContextA = createPositionDrivers("A");
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        temporaryContextA.destroy();
        assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
        try
        {
            PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("A")).forceResolve();
            fail("should not get here");
        }
        catch (MithraBusinessException e)
        {
            // ok
        }
        temporaryContextB.destroy();
    }

    public void testCreateAndDestroyWithSourceInheritedThread()
    {
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        try
        {
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
            final boolean[] good = new boolean[1];
            Thread thread = new Thread(new Runnable()
            {
                public void run()
                {
                    good[0] = PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size() == 100;
                }
            });
            thread.start();
            try
            {
                thread.join();
            }
            catch (InterruptedException e)
            {
                //ignore
            }
            assertTrue(good[0]);
        }
        finally
        {
            temporaryContextB.destroy();
        }
    }

    public void testCreateAndDestroyWithSourceInheritedThreadExecutor()
    {
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        try
        {
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
            final CountDownLatch latch = new CountDownLatch(9);
            ThreadConservingExecutor executor = new ThreadConservingExecutor(10);
            for(int i=0;i<10;i++)
            {
                executor.submit(new Runnable()
                {
                    public void run()
                    {
                        latch.countDown();
                        assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
                    }
                });
            }
            try
            {
                latch.await(10, TimeUnit.SECONDS);
                assertEquals(0, latch.getCount());
            }
            catch (InterruptedException e)
            {
                //ignore
            }
            executor.finish();
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
        }
        finally
        {
            temporaryContextB.destroy();
        }
    }

    public void testCreateAndDestroyWithSourceInheritedThreadExecutor2()
    {
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        try
        {
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
            ThreadConservingExecutor executor = new ThreadConservingExecutor(2);
            for(int i=0;i<10;i++)
            {
                executor.submit(new Runnable()
                {
                    public void run()
                    {
                        assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
                    }
                });
            }
            executor.finish();
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
        }
        finally
        {
            temporaryContextB.destroy();
        }
    }

    public void testCreateAndDestroyWithSourceInheritedThreadAutoShutdownExecutor()
    {
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        try
        {
            assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
            AutoShutdownThreadExecutor executor = new AutoShutdownThreadExecutor(10, "test");
            for(int i=0;i<10;i++)
            {
                executor.submit(new Runnable()
                {
                    public void run()
                    {
                        assertEquals(100, PositionDriverFinder.findMany(PositionDriverFinder.acmapCode().eq("B")).size());
                    }
                });
            }
            executor.shutdownAndWaitUntilDone();
        }
        finally
        {
            temporaryContextB.destroy();
        }
    }

    public void testCreateAndDestroyInTransactionWithSource()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCreateAndDestoryWithSource();
                return null;
            }
        });
    }

    public void testCreateAndDestroyInTransactionWithSourceTwice()
    {
        assertNull(MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCreateAndDestoryWithSource();
                testCreateAndDestoryWithSource();
                return null;
            }
        }));
        assertFalse(MithraManagerProvider.getMithraManager().isInTransaction());
    }

    public void testExecuteBufferedOperationsFails()
    {
        try
        {
            createTempContextAndInsertInTransaction(1); // should fail with duplicate index exception
            fail();
        }
        catch (MithraUniqueIndexViolationException e)
        {
            //expected
        }
        createTempContextAndInsertInTransaction(1000); // should not fail

    }

    private void createTempContextAndInsertInTransaction(final int orderId)
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TemporaryContext temporaryContext = createPositionDrivers("A");
                try
                {
                    Order o = new Order();
                    o.setOrderId(orderId); // if duplicate id, will fail on insert
                    o.insert();
                }
                finally
                {
                    temporaryContext.destroy();
                }
                return null;
            }
        });
    }

    public void testCreateInTransactionWithSource()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TemporaryContext temporaryContext = createPositionDrivers("A");
                return null;
            }
        });
    }

    public void testMultiThreadedLoaderWithTempInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testMultiThreadedLoaderWithTemp();
                return null;
            }
        });
    }

    public void testCreateAndDestroyInTransaction()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                testCreateAndDestroy();
                return null;
            }
        });
    }

    public void testCreateAndDestroyWithTwoSources()
    {
        TemporaryContext temporaryContextA = createPositionDrivers("A");
        TemporaryContext temporaryContextB = createPositionDrivers("B");
        temporaryContextA.destroy();
        temporaryContextB.destroy();
    }

    private TemporaryContext createPositionDrivers(String source)
    {
        TemporaryContext temporaryContext = PositionDriverFinder.createTemporaryContext(source);
        PositionDriverList list = new PositionDriverList();
        for(int i=0;i<100;i++)
        {
            list.add(createPositionDriver(source, "A"+i, i));
        }
        list.insertAll();
        return temporaryContext;
    }

    public void testCreateAndDestroy()
    {
        OrderFinder.findOne(OrderFinder.orderId().eq(1)).delete();
        TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();
        OrderDriverList list = new OrderDriverList();
        for(int i=0;i<100;i++)
        {
            OrderDriver od = new OrderDriver();
            od.setOrderId(i);
            list.add(od);
        }
        list.insertAll();
        OrderDriverList newList = new OrderDriverList(OrderDriverFinder.all());
        assertEquals(100, newList.size());
        temporaryContext.destroy();
    }

    public void testRelatedObjectExistsWithJoin()
    {
        TemporaryContext temporaryContext = OrderDriverFinder.createTemporaryContext();
        OrderDriverList list = new OrderDriverList();
        for(int i=1;i<3;i++)
        {
            OrderDriver od = new OrderDriver();
            od.setOrderId(i);
            list.add(od);
        }
        list.insertAll();
        Operation op = OrderDriverFinder.existsWithJoin(OrderItemFinder.order().orderId());
        OrderItemList items = new OrderItemList(op);
        assertEquals(4, items.size());
        temporaryContext.destroy();
    }

    public void testRelationshipWithTempObject()
    {
        TemporaryContext temporaryContext = null;
        try
        {
            temporaryContext = PositionDriverFinder.createTemporaryContext("A");
            createPositionDriverA();
            Operation op = PositionQuantityFinder.acmapCode().eq("A");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionQuantityFinder.positionDriver().exists());
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(5, pqList.size());
        }
        finally
        {
            if (temporaryContext != null)
            {
                temporaryContext.destroy();
            }
        }
    }

    public void testExistsWithJoinWithTempObject()
    {
        TemporaryContext temporaryContext = null;
        try
        {
            temporaryContext = PositionDriverFinder.createTemporaryContext("A");
            createPositionDriverA();
            Operation op = PositionQuantityFinder.acmapCode().eq("A");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionDriverFinder.existsWithJoin(PositionQuantityFinder.acmapCode(), PositionQuantityFinder.accountId(), PositionQuantityFinder.productId()));
            PositionQuantityList pqList = new PositionQuantityList(op);
            assertEquals(5, pqList.size());
        }
        finally
        {
            if (temporaryContext != null)
            {
                temporaryContext.destroy();
            }
        }
    }

    public void testExistsWithJoinViaRelationshipWithTempObjectMixedMapper()
    {
        TemporaryContext temporaryContext = null;
        try
        {
            temporaryContext = PositionDriverFinder.createTemporaryContext("A");
            createPositionDriverA();
            Operation op = PositionAdjustmentHistoryFinder.acmapCode().eq("A");
            op = op.and(PositionDriverFinder.existsWithJoin(PositionAdjustmentHistoryFinder.acmapCode(),
                    PositionAdjustmentHistoryFinder.positionQuantity().accountId(), PositionAdjustmentHistoryFinder.positionQuantity().productId()));
            PositionAdjustmentHistoryList pqList = new PositionAdjustmentHistoryList(op);
            assertEquals(5, pqList.size());
        }
        finally
        {
            if (temporaryContext != null)
            {
                temporaryContext.destroy();
            }
        }
    }

    public void testExistsWithJoinViaRelationshipWithTempObject()
    {
        TemporaryContext temporaryContext = null;
        try
        {
            temporaryContext = PositionDriverFinder.createTemporaryContext("A");
            createPositionDriverA();
            Operation op = PositionAdjustmentHistoryFinder.acmapCode().eq("A");
            op = op.and(PositionDriverFinder.existsWithJoin(PositionAdjustmentHistoryFinder.positionQuantity().acmapCode(),
                    PositionAdjustmentHistoryFinder.positionQuantity().accountId(), PositionAdjustmentHistoryFinder.positionQuantity().productId()));
            PositionAdjustmentHistoryList pqList = new PositionAdjustmentHistoryList(op);
            assertEquals(5, pqList.size());
        }
        finally
        {
            if (temporaryContext != null)
            {
                temporaryContext.destroy();
            }
        }
    }

    public void testMultipleThreadsWithTempObject()
    {

        final Exchanger<TemporaryContext> oneToTwo = new Exchanger<TemporaryContext>();
        final Exchanger<TemporaryContext> twoToThree = new Exchanger<TemporaryContext>();
        final Exchanger threeToOne = new Exchanger();

        ExceptionHandlingTask task1 = new ExceptionHandlingTask()
        {
            @Override
            public void execute()
            {
                TemporaryContext temporaryContext = PositionDriverFinder.createTemporaryContext("A");
                try
                {
                    oneToTwo.exchange(temporaryContext, 10, TimeUnit.SECONDS);
                    threeToOne.exchange(null, 10, TimeUnit.SECONDS);
                    temporaryContext.destroy();
                    temporaryContext = null;
                    threeToOne.exchange(null, 10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    if (temporaryContext != null)
                    {
                        temporaryContext.destroy();
                    }
                }

            }
        };

        ExceptionHandlingTask task2 = new ExceptionHandlingTask()
        {
            @Override
            public void execute()
            {
                try
                {
                    TemporaryContext temporaryContext = oneToTwo.exchange(null, 10, TimeUnit.SECONDS);
                    temporaryContext.associateToCurrentThread();
                    createPositionDriverA();
                    twoToThree.exchange(temporaryContext, 10, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };

        ExceptionHandlingTask task3 = new ExceptionHandlingTask()
        {
            @Override
            public void execute()
            {
                try
                {
                    TemporaryContext temporaryContext = twoToThree.exchange(null, 10, TimeUnit.SECONDS);
                    temporaryContext.associateToCurrentThread();
                    Operation op = PositionAdjustmentHistoryFinder.acmapCode().eq("A");
                    op = op.and(PositionDriverFinder.existsWithJoin(PositionAdjustmentHistoryFinder.positionQuantity().acmapCode(),
                            PositionAdjustmentHistoryFinder.positionQuantity().accountId(), PositionAdjustmentHistoryFinder.positionQuantity().productId()));
                    PositionAdjustmentHistoryList pqList = new PositionAdjustmentHistoryList(op);
                    assertEquals(5, pqList.size());
                    threeToOne.exchange(null, 10, TimeUnit.SECONDS);
                    threeToOne.exchange(null, 10, TimeUnit.SECONDS);
                    testExistsWithJoinViaRelationshipWithTempObject();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };

        Thread t1 = new Thread(task1, "Task 1");
        Thread t2 = new Thread(task2, "Task 2");
        Thread t3 = new Thread(task3, "Task 3");

        t1.start();
        t2.start();
        t3.start();

        task1.waitUntilDoneWithExceptionHandling();
        task2.waitUntilDoneWithExceptionHandling();
        task3.waitUntilDoneWithExceptionHandling();

    }

    public void testMultiThreadedLoaderWithTemp()
    {
        TemporaryContext temporaryContextA = null;
        TemporaryContext temporaryContextB = null;
        Throwable destroyExceptionA = null;
        Throwable destroyExceptionB = null;
        try
        {
            temporaryContextA = PositionDriverFinder.createTemporaryContext("A");
            createPositionDriverA();
            temporaryContextB = PositionDriverFinder.createTemporaryContext("B");
            createPositionDriverB();
            Operation op = PositionQuantityFinder.acmapCode().eq("A");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionQuantityFinder.positionDriver().exists());
            PositionQuantityList pqListA = new PositionQuantityList(op);

            op = PositionQuantityFinder.acmapCode().eq("B");
            op = op.and(PositionQuantityFinder.businessDate().eq(newTimestamp("2008-01-01 00:00:00")));
            op = op.and(PositionQuantityFinder.positionDriver().exists());
            PositionQuantityList pqListB = new PositionQuantityList(op);

            ArrayList listOfLists = new ArrayList();
            listOfLists.add(pqListA);
            listOfLists.add(pqListB);

            MithraMultiThreadedLoader loader = new MithraMultiThreadedLoader(3);
            loader.loadMultipleLists(listOfLists);
            int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
            assertEquals(5, pqListA.size());
            assertEquals(5, pqListB.size());
            if (!MithraManagerProvider.getMithraManager().isInTransaction())
            {
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
            }
        }
        catch(Throwable t)
        {
            getLogger().error("error in test testMultiThreadedLoaderWithTemp", t);
            throw new RuntimeException("error in test testMultiThreadedLoaderWithTemp", t);
        }
        finally
        {
            if (temporaryContextA != null)
            {
                try
                {
                    temporaryContextA.destroy();
                }
                catch (Throwable e)
                {
                    // we have to do this so we don't lose the exeption in the main part of the test
                    getLogger().error("destroy temp context failed", e);
                    destroyExceptionA = e;
                }
            }
            if (temporaryContextB != null)
            {
                try
                {
                    temporaryContextB.destroy();
                }
                catch (Throwable e)
                {
                    getLogger().error("destroy temp context failed", e);
                    destroyExceptionB = e;
                }
            }
        }
        // if we get here, there wasn't another exception above.
        if (destroyExceptionA != null) fail("destroy failed");
        if (destroyExceptionB != null) fail("destroy failed");
    }

    private void createPositionDriverA()
    {
        PositionDriverList list = new PositionDriverList();
        list.add(createPositionDriver("A", "7616150501", 1522));
        list.add(createPositionDriver("A", "7616030301", 1522));
        list.add(createPositionDriver("A", "7616030401", 1522));
        list.add(createPositionDriver("A", "7616030601", 1522));
        list.add(createPositionDriver("A", "7616030701", 1522));
        list.insertAll();
    }

    private void createPositionDriverB()
    {
        PositionDriverList list = new PositionDriverList();
        list.add(createPositionDriver("B", "7616150502", 1521));
        list.add(createPositionDriver("B", "7616030302", 1521));
        list.add(createPositionDriver("B", "7616030402", 1521));
        list.add(createPositionDriver("B", "7616030602", 1521));
        list.add(createPositionDriver("B", "7616030702", 1521));
        list.insertAll();
    }

    private PositionDriver createPositionDriver(String source, String account, int productId)
    {
        PositionDriver pd = new PositionDriver();
        pd.setAcmapCode(source);
        pd.setAccountId(account);
        pd.setProductId(productId);
        return pd;
    }

    private ParaProductDriver createParaProductDriver(String source, String gsn, String cusip)
    {
        ParaProductDriver driver = new ParaProductDriver();
        driver.setAcmapCode(source);
        driver.setGsn(gsn);
        driver.setCusip(cusip);
        return driver;
    }

}
