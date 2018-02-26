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

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.util.SingleQueueExecutor;
import com.gs.fw.common.mithra.util.ExecutorErrorHandler;
import com.gs.fw.common.mithra.util.DefaultExecutorErrorHandler;
import com.gs.fw.common.mithra.behavior.txparticipation.MithraOptimisticLockException;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;
import org.slf4j.Logger;

import java.sql.*;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ThreadPoolExecutor;
import java.text.SimpleDateFormat;
import java.text.ParseException;


public class TestOptimisticTransactionParticipation extends MithraTestAbstract
{

    private static final Timestamp originalUpdateTime;

    static
    {
        try
        {
            originalUpdateTime = new Timestamp(new SimpleDateFormat("yyyyMMdd").parse("20000101").getTime());
        }
        catch (ParseException e)
        {
            throw new RuntimeException("could not parse date", e);
        }
    }

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            TinyBalance.class, OptimisticOrder.class, OptimisticOrderWithTimestamp.class, AuditedOrder.class
        };
    }

    private TinyBalance findTinyBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return TinyBalanceFinder.findOne(TinyBalanceFinder.acmapCode().eq("A")
                            .and(TinyBalanceFinder.balanceId().eq(balanceId))
                            .and(TinyBalanceFinder.businessDate().eq(businessDate)));
    }

    private void updateDatedViaDirectSql()
    {
        int updated = 0;
        int inserted = 0;
        try
        {
            Connection con = this.getConnection();
            PreparedStatement ps = con.prepareStatement("update TINY_BALANCE set THRU_Z = '2006-07-03 22:17:31.400' , OUT_Z = '2006-07-03 22:17:31.400' where BALANCE_ID = 1 AND THRU_Z = ? AND OUT_Z = ?");
            ps.setTimestamp(1, InfinityTimestamp.getParaInfinity());
            ps.setTimestamp(2, InfinityTimestamp.getParaInfinity());
            updated = ps.executeUpdate();
            ps.close();
            Statement stm = con.createStatement();
            inserted = stm.executeUpdate("insert into TINY_BALANCE (BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values (1,-6851.0,'2002-11-29 00:00:00.000','2006-07-03 22:17:31.256','2006-07-03 22:17:31.400','9999-12-01 23:59:00.000')");
            inserted += stm.executeUpdate("insert into TINY_BALANCE (BALANCE_ID,POS_QUANTITY_M,FROM_Z,THRU_Z,IN_Z,OUT_Z) values (1,12.5,'2006-07-03 22:17:31.256','9999-12-01 23:59:00.000','2006-07-03 22:17:31.400','9999-12-01 23:59:00.000')");
            stm.close();
            con.close();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("unexpected sql exception", e);
        }
        assertEquals(1, updated);
        assertEquals(2, inserted);
    }

    public void testAuditOnlyOptimisticTerminate()
    {
        final AuditedOrder order = AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(1));
        assertNotNull(order);
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                order.terminate();
                return null;
            }
        });

        assertNull(AuditedOrderFinder.findOneBypassCache(AuditedOrderFinder.orderId().eq(1)));
    }

    public void testDatedOptimisticLockFailure()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
                final int balanceId = 1;
                final TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
                assertNotNull(tb);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        TinyBalance fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(-6851, tb.getQuantity(), 0.0);
                        }
                        else
                        {
                            assertEquals(12.5, tb.getQuantity(), 0.0);
                        }
                        tb.setQuantity(101.4);
                        assertTrue(fromCache.getQuantity() == 101.4);
                        assertTrue(tb.zIsParticipatingInTransaction(tx));
                        fromCache = findTinyBalanceForBusinessDate(balanceId, businessDate);
                        assertSame(tb, fromCache);
                        fromCache = findTinyBalanceForBusinessDate(balanceId, new Timestamp(businessDate.getTime()+1000));
                        assertNotSame(tb, fromCache);
                        assertTrue(fromCache.getQuantity() == 101.4);
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    private void updateNonDatedViaDirectSql()
    {
        int updated = 0;
        try
        {
            Connection con = this.getConnection();
            PreparedStatement ps = con.prepareStatement("update OPTIMISTIC_ORDER set TRACKING_ID = 'abc', VERSION = 2 where ORDER_ID = 1");
            updated = ps.executeUpdate();
            ps.close();
            con.close();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("unexpected sql exception", e);
        }
        assertEquals(1, updated);
    }

    private void updateNonDatedViaDirectSqlWithTimestamp()
    {
        int updated = 0;
        try
        {
            Connection con = this.getConnection();
            Timestamp now = new Timestamp(System.currentTimeMillis());
            PreparedStatement ps = con.prepareStatement("update OPTIMISTIC_ORDER_WITH_TIMESTAMP set TRACKING_ID = 'abc', UPDATE_TIME = ? where ORDER_ID = 1");
            ps.setTimestamp(1, now);
            updated = ps.executeUpdate();
            ps.close();
            con.close();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("unexpected sql exception", e);
        }
        assertEquals(1, updated);
    }

    public void testNonDatedOptimisticLockFailure()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        assertTrue(order.getVersion() > 0);
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                        assertSame(order, order2);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testSingleQueueExecutorWithOptimisticFailure()
    {
        if (OptimisticOrderFinder.getMithraObjectPortal().getCache().isFullCache()) return;
        SingleQueueExecutor executor = new SingleQueueExecutor(1, OptimisticOrderFinder.orderId().ascendingOrderBy(),
                10, OptimisticOrderFinder.getFinderInstance(), 1)
        {
            @Override
            protected void setTransactionOptions(MithraTransaction tx)
            {
                super.setTransactionOptions(tx);
                tx.setRetryOnOptimisticLockFailure(false);
            }
        };
        executor.setErrorHandler(new ExecutorErrorHandler()
        {
            ExecutorErrorHandler defaultHandler =  new DefaultExecutorErrorHandler();
            public void handle(Throwable t, Logger logger, SingleQueueExecutor sqe, ThreadPoolExecutor executor, SingleQueueExecutor.CallableTask task)
            {
                if (t instanceof MithraOptimisticLockException)
                {
                    for(SingleQueueExecutor.TransactionOperation op: task.getOperations())
                    {
                        ((OptimisticOrder)op.getTxObject()).getOrderId(); // any get method will cause the object to refresh if it needs it
                    }
                    executor.getQueue().add(new SingleQueueExecutor.CallableWrapper(task, sqe, sqe.getCounterForTask(task), executor));
                }
                else
                {
                    defaultHandler.handle(t, logger, sqe, executor, task);
                }
            }
        });
        OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        OptimisticOrder copy = order.getNonPersistentCopy();
        copy.setState("new state");
        assertNotNull(order);
        updateNonDatedViaDirectSql();
        executor.addForUpdate(order, copy);
        executor.waitUntilFinished();
        assertEquals("new state", order.getState());
    }

    public void testSingleQueueExecutorWithOptimisticFailureWithoutErrorHandler()
    {
        if (OptimisticOrderFinder.getMithraObjectPortal().getCache().isFullCache()) return;
        // this test gets lucky because the SQE attempts to print a log message, which happens to refresh the object.
        SingleQueueExecutor executor = new SingleQueueExecutor(1, OptimisticOrderFinder.orderId().ascendingOrderBy(),
                10, OptimisticOrderFinder.getFinderInstance(), 1);
        OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        OptimisticOrder copy = order.getNonPersistentCopy();
        copy.setState("new state");
        assertNotNull(order);
        updateNonDatedViaDirectSql();
        executor.addForUpdate(order, copy);
        executor.waitUntilFinished();
        assertEquals("new state", order.getState());
    }

    public void testUpdate()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertEquals(1, order.getVersion());
                order.setState("new state");
                assertEquals(2, order.getVersion());
                return null;
            }
        });
        OptimisticOrder order = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(2, order.getVersion());
        assertEquals("new state", order.getState());
    }

    public void testUpdateTwoAttributes()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                OptimisticOrderFinder.setTransactionModeFullTransactionParticipation(tx);
                assertEquals(1, order.getVersion());
                order.setState("new state");
                order.setTrackingId("new tid14");
                assertEquals(2, order.getVersion());
                return null;
            }
        });
        OptimisticOrder order = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(2, order.getVersion());
        assertEquals("new state", order.getState());
        assertEquals("new tid14", order.getTrackingId());
    }

    public void testSetVersionNotAllowed()
    {
        OptimisticOrder order = new OptimisticOrder();
        order.setVersion(1000); // allowed before insert
        assertEquals(1000, order.getVersion());
        order.setOrderId(120);
        order.insert();
        try
        {
            order.setVersion(2000);
            fail("must not be able to set version");
        }
        catch (MithraBusinessException e)
        {
            // ok
        }
    }

    public void testUpdateWithExecuteBufferedOperations()
    {
        final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, order.getVersion());
                order.setState("new state");
                tx.executeBufferedOperations();
                order.setTrackingId("new tid14");
                assertEquals(3, order.getVersion());
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(3, order2.getVersion());
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
    }

    public void testBatchUpdateOneAttribute()
    {
        final OptimisticOrder orderA = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder orderB = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, orderA.getVersion());
                orderA.setState("new state");
                assertEquals(2, orderA.getVersion());
                orderB.setState("new state 2");
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(2, order2.getVersion());
        assertEquals("new state", order2.getState());
        order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(2));
        assertEquals("new state 2", order2.getState());
    }

    public void testBatchUpdateTwoAttributes()
    {
        final OptimisticOrder orderA = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder orderB = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, orderA.getVersion());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertEquals(2, orderA.getVersion());
                orderB.setState("new state 2");
                orderB.setTrackingId("new tid15");
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(2, order2.getVersion());
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(2));
        assertEquals("new state 2", order2.getState());
        assertEquals("new tid15", order2.getTrackingId());
    }

    public void testBatchUpdateTwoAttributesAndExecuteBufferedOperations()
    {
        final OptimisticOrder orderA = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder orderB = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, orderA.getVersion());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertEquals(2, orderA.getVersion());
                orderB.setState("new state 2");
                orderB.setTrackingId("new tid15");
                tx.executeBufferedOperations();
                orderA.setUserId(17);
                orderB.setUserId(19);
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(3, order2.getVersion());
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
        order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(2));
        assertEquals("new state 2", order2.getState());
        assertEquals("new tid15", order2.getTrackingId());
        assertEquals(19, order2.getUserId());
    }

    public void testMultiUpdateTwoAttributes()
    {
        final OptimisticOrder orderA = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder orderB = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(3));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, orderA.getVersion());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertEquals(2, orderA.getVersion());
                assertEquals(1, orderB.getVersion());
                orderB.setState("new state");
                orderB.setTrackingId("new tid14");
                assertEquals(2, orderB.getVersion());
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(2, order2.getVersion());
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(3));
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
    }

    public void testMultiUpdateTwoAttributesWithExecuteBuffered()
    {
        final OptimisticOrder orderA = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder orderB = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(3));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, orderA.getVersion());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertEquals(2, orderA.getVersion());
                assertEquals(1, orderB.getVersion());
                orderB.setState("new state");
                orderB.setTrackingId("new tid14");
                assertEquals(2, orderB.getVersion());
                tx.executeBufferedOperations();
                orderA.setUserId(17);
                orderB.setUserId(17);
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertEquals(3, order2.getVersion());
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
        order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(3));
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
    }

    public void testOptimisticDelete()
    {
        final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, order.getVersion());
                order.delete();
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNull(order2);
    }

    private void deleteNonDatedViaDirectSql()
    {
        int updated = 0;
        try
        {
            Connection con = this.getConnection();
            PreparedStatement ps = con.prepareStatement("delete from OPTIMISTIC_ORDER where ORDER_ID = 1");
            updated = ps.executeUpdate();
            ps.close();
            con.close();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("unexpected sql exception", e);
        }
        assertEquals(1, updated);
    }

    public void testNonDatedOptimisticLockFailureForDelete()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        if (runThrough[0] == 1)
                        {
                            assertTrue(order.getVersion() > 0);
                            assertNotNull(order2);
                            assertSame(order, order2);
                            assertEquals(1, order2.getVersion());
                            order2.delete();
                        }
                        else
                        {
                            assertNull(order2);
                        }
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                deleteNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedOptimisticLockFailureForDeleteAfterUpdate()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        assertTrue(order.getVersion() > 0);
                        assertNotNull(order2);
                        assertSame(order, order2);
                        if (runThrough[0] == 1)
                        {
                            assertEquals(1, order2.getVersion());
                        }
                        else
                        {
                            assertEquals(2, order2.getVersion());
                        }
                        order2.delete();
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedOptimisticLockFailureForBatchDeleteAfterUpdate()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
                assertNotNull(order2);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        assertTrue(order.getVersion() > 0);
                        if (runThrough[0] == 1)
                        {
                            assertEquals(1, order.getVersion());
                        }
                        else
                        {
                            assertEquals(2, order.getVersion());
                        }
                        order.delete();
                        order2.delete();
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testOptimisticBatchDelete()
    {
        final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        final OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, order.getVersion());
                order.delete();
                order2.delete();
                return null;
            }
        });
        OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNull(order3);
        OptimisticOrder order4 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(2));
        assertNull(order4);
    }

    public void testOptimisticDeleteAfterUpdate()
    {
        final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, order.getVersion());
                order.setState("1234");
                order.delete();
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testOptimisticDeleteAfterUpdateWithExecuteBuffer()
    {
        final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(1, order.getVersion());
                order.setState("1234");
                tx.executeBufferedOperations();
                order.delete();
                return null;
            }
        });
        OptimisticOrder order2 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testNonDatedBatchUpdateOptimisticLockFailure()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(2));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        assertTrue(order.getVersion() > 0);
                        assertTrue(order2.getVersion() > 0);
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        order2.setState("some other state");
                        OptimisticOrder order3 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                        assertSame(order, order3);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
                OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
                assertEquals("new state", order3.getState());
                OptimisticOrder order4 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(2));
                assertEquals("some other state", order4.getState());
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedMultiUpdateOptimisticLockFailure()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrder order = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrder order2 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(3));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        assertTrue(order.getVersion() > 0);
                        assertTrue(order2.getVersion() > 0);
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        order2.setState("new state");
                        OptimisticOrder order3 = OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(1));
                        assertSame(order, order3);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
                OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
                assertEquals("new state", order3.getState());
                OptimisticOrder order4 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(3));
                assertEquals("new state", order4.getState());
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSql();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testUpdateNotAllowedOutsideTx()
    {
        OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        try
        {
            order3.setUserId(120);
            fail("must not be able to set attributes outside a transaction");
        }
        catch(MithraBusinessException e)
        {
            // ok
        }
    }

    public void testNonDatedOptimisticLockFailureWithTimestamp()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                        assertSame(order, order2);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testUpdateWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.setState("new state");
                assertTrue(order.getUpdateTime().getTime() > now);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order.getUpdateTime().getTime() > now);
        assertEquals("new state", order.getState());
    }

    public void testUpdateTwoAttributesWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                OptimisticOrderWithTimestampFinder.setTransactionModeFullTransactionParticipation(tx);
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.setState("new state");
                order.setTrackingId("new tid14");
                assertTrue(order.getUpdateTime().getTime() > now);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order.getUpdateTime().getTime() > now);
        assertEquals("new state", order.getState());
        assertEquals("new tid14", order.getTrackingId());
    }

    public void testSetVersionNotAllowedWithTimestamp()
    {
        OptimisticOrderWithTimestamp order = new OptimisticOrderWithTimestamp();
        Timestamp now = new Timestamp(System.currentTimeMillis());
        order.setUpdateTime(now);
        assertEquals(now, order.getUpdateTime());
        order.setOrderId(120);
        order.insert();
        try
        {
            order.setUpdateTime(new Timestamp(1234568L));
            fail("must not be able to set update time");
        }
        catch (MithraBusinessException e)
        {
            // ok
        }
    }

    public void testUpdateWithExecuteBufferedOperationsWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.setState("new state");
                tx.executeBufferedOperations();
                order.setTrackingId("new tid14");
                assertTrue(order.getUpdateTime().getTime() > now);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order.getUpdateTime().getTime() > now);
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
    }

    public void testBatchUpdateTwoAttributesWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final OptimisticOrderWithTimestamp orderA = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        final OptimisticOrderWithTimestamp orderB = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, orderA.getUpdateTime());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertTrue(orderA.getUpdateTime().getTime() > now);
                orderB.setState("new state 2");
                orderB.setTrackingId("new tid15");
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order2.getUpdateTime().getTime() > now);
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        assertEquals("new state 2", order2.getState());
        assertEquals("new tid15", order2.getTrackingId());
    }

    public void testBatchUpdateTwoAttributesAndExecuteBufferedOperationsWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final OptimisticOrderWithTimestamp orderA = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        final OptimisticOrderWithTimestamp orderB = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, orderA.getUpdateTime());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertTrue(orderA.getUpdateTime().getTime() > now);
                orderB.setState("new state 2");
                orderB.setTrackingId("new tid15");
                tx.executeBufferedOperations();
                orderA.setUserId(17);
                orderB.setUserId(19);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order2.getUpdateTime().getTime() > now);
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
        order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        assertEquals("new state 2", order2.getState());
        assertEquals("new tid15", order2.getTrackingId());
        assertEquals(19, order2.getUserId());
    }

    public void testMultiUpdateTwoAttributesWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final OptimisticOrderWithTimestamp orderA = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        final OptimisticOrderWithTimestamp orderB = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(3));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, orderA.getUpdateTime());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertTrue(orderA.getUpdateTime().getTime() > now);
                assertEquals(originalUpdateTime, orderB.getUpdateTime());
                orderB.setState("new state");
                orderB.setTrackingId("new tid14");
                assertTrue(orderB.getUpdateTime().getTime() > now);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order2.getUpdateTime().getTime() > now);
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(3));
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
    }

    public void testMultiUpdateTwoAttributesWithExecuteBufferedWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final OptimisticOrderWithTimestamp orderA = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        final OptimisticOrderWithTimestamp orderB = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(3));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, orderA.getUpdateTime());
                orderA.setState("new state");
                orderA.setTrackingId("new tid14");
                assertTrue(orderA.getUpdateTime().getTime() > now);
                assertEquals(originalUpdateTime, orderB.getUpdateTime());
                orderB.setState("new state");
                orderB.setTrackingId("new tid14");
                assertTrue(orderB.getUpdateTime().getTime() > now);
                tx.executeBufferedOperations();
                orderA.setUserId(17);
                orderB.setUserId(17);
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertTrue(order2.getUpdateTime().getTime() > now);
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
        order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(3));
        assertEquals("new state", order2.getState());
        assertEquals("new tid14", order2.getTrackingId());
        assertEquals(17, order2.getUserId());
    }

    public void testOptimisticDeleteWithTimestamp()
    {
        final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.delete();
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertNull(order2);
    }

    private void deleteNonDatedViaDirectSqlWithTimestamp()
    {
        int updated = 0;
        try
        {
            Connection con = this.getConnection();
            PreparedStatement ps = con.prepareStatement("delete from OPTIMISTIC_ORDER_WITH_TIMESTAMP where ORDER_ID = 1");
            updated = ps.executeUpdate();
            ps.close();
            con.close();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("unexpected sql exception", e);
        }
        assertEquals(1, updated);
    }

    public void testNonDatedOptimisticLockFailureForDeleteWithTimestamp()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        if (runThrough[0] == 1)
                        {
                            assertNotNull(order2);
                            assertSame(order, order2);
                            assertEquals(originalUpdateTime, order2.getUpdateTime());
                            order2.delete();
                        }
                        else
                        {
                            assertNull(order2);
                        }
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                deleteNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedOptimisticLockFailureForDeleteAfterUpdateWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        assertNotNull(order2);
                        assertSame(order, order2);
                        if (runThrough[0] == 1)
                        {
                            assertEquals(originalUpdateTime, order2.getUpdateTime());
                        }
                        else
                        {
                            assertTrue(order2.getUpdateTime().getTime() > now);
                        }
                        order2.delete();
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedOptimisticLockFailureForBatchDeleteAfterUpdateWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(2));
                assertNotNull(order2);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        if (runThrough[0] == 1)
                        {
                            assertEquals(originalUpdateTime, order.getUpdateTime());
                        }
                        else
                        {
                            assertTrue(order.getUpdateTime().getTime() > now);
                        }
                        order.delete();
                        order2.delete();
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testOptimisticBatchDeleteWithTimestamp()
    {
        final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        final OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.delete();
                order2.delete();
                return null;
            }
        });
        OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertNull(order3);
        OptimisticOrderWithTimestamp order4 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(2));
        assertNull(order4);
    }

    public void testOptimisticDeleteAfterUpdateWithTimestamp()
    {
        final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.setState("1234");
                order.delete();
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testOptimisticDeleteAfterUpdateWithExecuteBufferWithTimestamp()
    {
        final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                assertEquals(originalUpdateTime, order.getUpdateTime());
                order.setState("1234");
                tx.executeBufferedOperations();
                order.delete();
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        assertNull(order2);
    }

    public void testNonDatedBatchUpdateOptimisticLockFailureWithTimestamp()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(2));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        order2.setState("some other state");
                        OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                        assertSame(order, order3);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
                OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertEquals("new state", order3.getState());
                OptimisticOrderWithTimestamp order4 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(2));
                assertEquals("some other state", order4.getState());
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testNonDatedMultiUpdateOptimisticLockFailureWithTimestamp()
    {
        final Exchanger rendezvous = new Exchanger();
        Runnable runnable1 = new Runnable()
        {
            public void run()
            {
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertNotNull(order);
                final OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(3));
                assertNotNull(order);
                waitForOtherThread(rendezvous); // 1
                waitForOtherThread(rendezvous); // 2
                final int runThrough[] = new int[1];
                MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        runThrough[0]++;
                        tx.setRetryOnOptimisticLockFailure(true);
                        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                        if (runThrough[0] == 1)
                        {
                            assertEquals("123", order.getTrackingId());
                        }
                        else
                        {
                            assertEquals("abc", order.getTrackingId());
                        }
                        order.setState("new state");
                        order2.setState("new state");
                        OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                        assertSame(order, order3);
                        assertTrue(order.zIsParticipatingInTransaction(tx));
                        if (runThrough[0] == 1)
                        {
                            assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                        }
                        return null;
                    }
                });
                assertEquals(2, runThrough[0]);
                OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
                assertEquals("new state", order3.getState());
                OptimisticOrderWithTimestamp order4 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(3));
                assertEquals("new state", order4.getState());
            }
        };

        Runnable runnable2 = new Runnable()
        {
            public void run()
            {
                waitForOtherThread(rendezvous); // 1
                updateNonDatedViaDirectSqlWithTimestamp();
                waitForOtherThread(rendezvous); // 2
            }
        };
        if (TinyBalanceFinder.getMithraObjectPortal().getCache().isPartialCache())
        {
            assertTrue(this.runMultithreadedTest(runnable1, runnable2));
        }
    }

    public void testOptimisticInsertWithTimestamp()
    {
        final long now = System.currentTimeMillis() - 10;
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                final OptimisticOrderWithTimestamp order = new OptimisticOrderWithTimestamp();
                order.setOrderId(5000);
                order.setOrderDate(new Timestamp(now));
                order.setState("1234");
                order.insert();
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(5000));
        assertNotNull(order2);
        assertTrue(order2.getUpdateTime().getTime() > now);
    }

    public void testUpdateNotAllowedOutsideTxWithTimestamp()
    {
        OptimisticOrderWithTimestamp order3 = OptimisticOrderWithTimestampFinder.findOneBypassCache(OptimisticOrderWithTimestampFinder.orderId().eq(1));
        try
        {
            order3.setUserId(120);
            fail("must not be able to set attributes outside a transaction");
        }
        catch(MithraBusinessException e)
        {
            // ok
        }
    }

    public void testOptimisticDeletedOfNonDatedObjectCreatedOutsideTx()
    {
        final int orderId = 10000;
        OptimisticOrderWithTimestamp order1 = new OptimisticOrderWithTimestamp();
        order1.setOrderId(orderId);
        order1.setState("test");
        order1.setDescription("test");
        order1.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order1.setTrackingId("test");
        order1.setUserId(1);
        order1.insert();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = OptimisticOrderWithTimestampFinder.orderId().eq(orderId);
                final OptimisticOrderWithTimestamp order = OptimisticOrderWithTimestampFinder.findOneBypassCache(op);
                if(order != null)
                {
                    order.delete();
                }
                return null;
            }
        });
        OptimisticOrderWithTimestamp order2= OptimisticOrderWithTimestampFinder.findOne(OptimisticOrderWithTimestampFinder.orderId().eq(orderId));
        assertNull(order2);
    }

    public void testCreatingNonDatedOutsideTxInsertingInTx()
    {
        final int orderId = 10000;
        final OptimisticOrder order = new OptimisticOrder();
        order.setOrderId(orderId);
        order.setOrderDate(new Timestamp(System.currentTimeMillis()));
        order.setDescription("Test");
        order.setState("State");
        order.setTrackingId("Tracking");
        order.setUserId(9999);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                order.insert();
                return null;
            }
        });

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = OptimisticOrderFinder.orderId().eq(orderId);
                final OptimisticOrder order = OptimisticOrderFinder.findOneBypassCache(op);
                if(order != null)
                {
                    order.delete();
                }
                return null;
            }
        });
        OptimisticOrder order2= OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(orderId));
        assertNull(order2);

    }

    public void testDatedOptimisitcLockExceptionWithDetached()
    {
        final Timestamp businessDate = new Timestamp(System.currentTimeMillis());
        final int balanceId = 1;
        final TinyBalance tb = findTinyBalanceForBusinessDate(balanceId, businessDate);
        assertNotNull(tb);
        final TinyBalance detachedOne = tb.getDetachedCopy();
        detachedOne.setQuantity(102);

        final TinyBalance detachedTwo = tb.getDetachedCopy();
        detachedTwo.setQuantity(105);

        detachedOne.copyDetachedValuesToOriginalOrInsertIfNew();
        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                    detachedTwo.copyDetachedValuesToOriginalOrInsertIfNew();
                    return null;
                }
            });
            fail("should not get here");
        }
        catch(MithraOptimisticLockException e)
        {
            // ok
        }
    }

    public void testOptimisitcLockExceptionWithDetached()
    {
        final OptimisticOrder order3 = OptimisticOrderFinder.findOneBypassCache(OptimisticOrderFinder.orderId().eq(1));
        assertNotNull(order3);

        final OptimisticOrder detachedTwo = order3.getDetachedCopy();
        detachedTwo.setUserId(105);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                order3.setUserId(12);
                return null;
            }
        });

        try
        {
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
            {
                public Object executeTransaction(MithraTransaction tx) throws Throwable
                {
                    TinyBalanceFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);
                    detachedTwo.copyDetachedValuesToOriginalOrInsertIfNew();
                    return null;
                }
            });
            fail("should not get here");
        }
        catch(MithraOptimisticLockException e)
        {
            // ok
        }
    }

    public void testNonDatedOptimisticIndexQueryAfterInsert()
    {
        // multiple orders by index
        queryOptimisticOrderBeforeAndAfterInsert(1, 3, 100);
        // single order by index
        queryOptimisticOrderBeforeAndAfterInsert(2, 1, 101);
        // no orders by index
        queryOptimisticOrderBeforeAndAfterInsert(3, 0, 102);
    }

    private void queryOptimisticOrderBeforeAndAfterInsert(final int userIdToInsertAndQuery, final int sizeBefore, final int orderIdToInsert) {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                OptimisticOrderList originalOrders = OptimisticOrderFinder.findMany(OptimisticOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore, originalOrders.size());

                OptimisticOrder toInsert = new OptimisticOrder();
                toInsert.setOrderId(orderIdToInsert);
                toInsert.setUserId(userIdToInsertAndQuery);
                toInsert.setDescription("Just Inserted");
                toInsert.setState("New");
                toInsert.insert();

                OptimisticOrderList updatedOrders = OptimisticOrderFinder.findMany(OptimisticOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore + 1, updatedOrders.size());

                return null;
            }
        });
    }

    public void testDatedOptimisticIndexQueryAfterInsert()
    {
        // multiple orders by index
        queryAuditedOrderBeforeAndAfterInsert(1, 5, 100);
        // single order by index
        queryAuditedOrderBeforeAndAfterInsert(2, 1, 101);
        // no orders by index
        queryAuditedOrderBeforeAndAfterInsert(3, 0, 102);
    }

    private void queryAuditedOrderBeforeAndAfterInsert(final int userIdToInsertAndQuery, final int sizeBefore, final int orderIdToInsert) {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                AuditedOrderList originalOrders = AuditedOrderFinder.findMany(AuditedOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore, originalOrders.size());

                AuditedOrder toInsert = new AuditedOrder();
                toInsert.setOrderId(orderIdToInsert);
                toInsert.setUserId(userIdToInsertAndQuery);
                toInsert.setDescription("Just Inserted");
                toInsert.setState("New");
                toInsert.insert();

                AuditedOrderList updatedOrders = AuditedOrderFinder.findMany(AuditedOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore + 1, updatedOrders.size());

                return null;
            }
        });
    }

    public void testNonDatedOptimisticIndexQueryAfterDelete()
    {
        // multiple orders by index
        queryOptimisticOrderBeforeAndAfterDelete(1, 3, 1);
        // single order by index
        queryOptimisticOrderBeforeAndAfterDelete(2, 1, 4);
    }

    private void queryOptimisticOrderBeforeAndAfterDelete(final int userIdToInsertAndQuery, final int sizeBefore, final int orderToDelete) {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                OptimisticOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                OptimisticOrderList originalOrders = OptimisticOrderFinder.findMany(OptimisticOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore, originalOrders.size());

                OptimisticOrderFinder.findOne(OptimisticOrderFinder.orderId().eq(orderToDelete)).delete();

                OptimisticOrderList updatedOrders = OptimisticOrderFinder.findMany(OptimisticOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore - 1, updatedOrders.size());

                return null;
            }
        });
    }

    public void testDatedOptimisticIndexQueryAfterDelete()
    {
        // multiple orders by index
        queryAuditedOrderBeforeAndAfterTerminate(1, 5, 1);
        // single order by index
        queryAuditedOrderBeforeAndAfterTerminate(2, 1, 4);
    }

    private void queryAuditedOrderBeforeAndAfterTerminate(final int userIdToInsertAndQuery, final int sizeBefore, final int orderToTerminate) {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                AuditedOrderFinder.setTransactionModeReadCacheWithOptimisticLocking(tx);

                AuditedOrderList originalOrders = AuditedOrderFinder.findMany(AuditedOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore, originalOrders.size());

                AuditedOrderFinder.findOne(AuditedOrderFinder.orderId().eq(orderToTerminate)).terminate();

                AuditedOrderList updatedOrders = AuditedOrderFinder.findMany(AuditedOrderFinder.userId().eq(userIdToInsertAndQuery));
                assertEquals(sizeBefore - 1, updatedOrders.size());

                return null;
            }
        });
    }
}
